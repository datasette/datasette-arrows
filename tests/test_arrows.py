from datasette.app import Datasette
import pyarrow as pa
import pytest
import pytest_asyncio


@pytest_asyncio.fixture
async def ds():
    datasette = Datasette(memory=True, settings={"default_page_size": 10})
    db = datasette.add_memory_database("data")
    await db.execute_write_script("""
        create table owners (id integer primary key, name text);
        insert into owners values (1, 'Cleo'), (2, 'Pancakes');
        create table dogs (
            id integer primary key,
            name text,
            weight real,
            photo blob,
            anything,
            owner_id integer references owners(id)
        );
        create table empty (id integer primary key, name text, score float);
        create table mixed (id integer primary key, value);
        create view dog_names as select id, name from dogs;
        """)
    await db.execute_write_many(
        "insert into dogs values (?, ?, ?, ?, ?, ?)",
        [
            (
                i,
                f"Dog {i}",
                i + 0.5,
                bytes([i % 256]) if i % 2 else None,
                None,
                1 + i % 2,
            )
            for i in range(1, 36)
        ],
    )
    await db.execute_write_many(
        "insert into mixed values (?, ?)",
        # The first page is all integers, later pages are not
        [(i, i) for i in range(1, 11)] + [(11, 1.5), (12, "text")],
    )
    await datasette.invoke_startup()
    return datasette


def read(response):
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/vnd.apache.arrow.stream"
    return pa.ipc.open_stream(response.content).read_all()


@pytest.mark.asyncio
async def test_plugin_is_installed():
    datasette = Datasette(memory=True)
    response = await datasette.client.get("/-/plugins.json")
    assert response.status_code == 200
    installed_plugins = {p["name"] for p in response.json()}
    assert "datasette-arrows" in installed_plugins


@pytest.mark.asyncio
async def test_table_page(ds):
    table = read(await ds.client.get("/data/dogs.arrows"))
    assert table.schema == pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("weight", pa.float64()),
            ("photo", pa.binary()),
            # No declared type and no values on the first page
            ("anything", pa.string()),
            ("owner_id", pa.int64()),
        ]
    )
    # Without ?_stream=1 it is just the first page
    assert table.num_rows == 10
    assert table.to_pylist()[0] == {
        "id": 1,
        "name": "Dog 1",
        "weight": 1.5,
        "photo": b"\x01",
        "anything": None,
        "owner_id": 2,
    }


@pytest.mark.asyncio
async def test_stream_all_rows(ds):
    response = await ds.client.get("/data/dogs.arrows?_stream=1")
    reader = pa.ipc.open_stream(response.content)
    batches = list(reader)
    # One record batch per page
    assert [batch.num_rows for batch in batches] == [10, 10, 10, 5]
    table = pa.Table.from_batches(batches)
    assert table["id"].to_pylist() == list(range(1, 36))
    assert table["name"].to_pylist()[-1] == "Dog 35"


@pytest.mark.asyncio
async def test_stream_respects_filters_and_sort(ds):
    table = read(
        await ds.client.get("/data/dogs.arrows?_stream=1&owner_id=1&_sort_desc=id")
    )
    assert table["id"].to_pylist() == list(range(34, 0, -2))


@pytest.mark.asyncio
async def test_view(ds):
    table = read(await ds.client.get("/data/dog_names.arrows?_stream=1"))
    assert table.column_names == ["id", "name"]
    assert table.num_rows == 35


@pytest.mark.asyncio
async def test_empty_table_uses_declared_types(ds):
    table = read(await ds.client.get("/data/empty.arrows?_stream=1"))
    assert table.num_rows == 0
    assert table.schema == pa.schema(
        [("id", pa.int64()), ("name", pa.string()), ("score", pa.float64())]
    )


@pytest.mark.asyncio
async def test_sql_query(ds):
    table = read(
        await ds.client.get(
            "/data/-/query.arrows",
            params={
                "sql": "select 1 as i, 1.5 as f, 'x' as s, x'00ff' as b, null as n "
                "union all select 2, 2, null, null, null"
            },
        )
    )
    assert table.schema == pa.schema(
        [
            ("i", pa.int64()),
            ("f", pa.float64()),
            ("s", pa.string()),
            ("b", pa.binary()),
            ("n", pa.string()),
        ]
    )
    assert table.to_pylist() == [
        {"i": 1, "f": 1.5, "s": "x", "b": b"\x00\xff", "n": None},
        {"i": 2, "f": 2.0, "s": None, "b": None, "n": None},
    ]


@pytest.mark.asyncio
async def test_sql_query_mixed_types_become_strings(ds):
    table = read(
        await ds.client.get(
            "/data/-/query.arrows",
            params={"sql": "select 1 as v union all select 'two' union all select 3.5"},
        )
    )
    assert table.schema == pa.schema([("v", pa.string())])
    assert sorted(table["v"].to_pylist()) == ["1", "3.5", "two"]


@pytest.mark.asyncio
async def test_sql_error(ds):
    response = await ds.client.get("/data/-/query.arrows", params={"sql": "select nope"})
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_row_page(ds):
    table = read(await ds.client.get("/data/dogs/3.arrows"))
    assert table["name"].to_pylist() == ["Dog 3"]


@pytest.mark.asyncio
async def test_labels(ds):
    table = read(await ds.client.get("/data/dogs.arrows?_labels=on&_size=2"))
    assert table.column_names == [
        "id",
        "name",
        "weight",
        "photo",
        "anything",
        "owner_id",
        "owner_id_label",
    ]
    assert table["owner_id"].to_pylist() == [2, 1]
    assert table["owner_id_label"].to_pylist() == ["Pancakes", "Cleo"]


@pytest.mark.asyncio
async def test_first_page_fits_wider_type(ds):
    # All 12 rows on the first page, so the column is typed to fit them all
    table = read(await ds.client.get("/data/mixed.arrows?_size=12"))
    assert table.schema.field("value").type == pa.string()
    assert table["value"].to_pylist()[-3:] == ["10", "1.5", "text"]


@pytest.mark.asyncio
async def test_later_page_that_does_not_fit_fails(ds):
    # The first page of ten rows is all integers, so the column is int64 -
    # the float and string that follow cannot be represented. The stream
    # fails rather than returning an incomplete file that looks complete.
    with pytest.raises(ValueError, match="Column 'value' is int64"):
        await ds.client.get("/data/mixed.arrows?_stream=1")


@pytest.mark.asyncio
async def test_export_dialog_lists_arrow(ds):
    response = await ds.client.get("/data/dogs")
    assert "/data/dogs.arrows" in response.text
