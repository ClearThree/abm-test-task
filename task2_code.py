import json
import time


async def create_tables(db):
    await db.execute("""create table error_status
    (
        occurred_at integer,
        object varchar,
        errors_tuple json,
        object_id integer default 0
    );
    create table object_status
    (
        occurred_at integer,
        online integer,
        ping integer,
        object varchar,
        obj_id integer default 0
    );
    """)


async def accept_status(db, **kwargs):
    errors = {}
    ping, online = None, None
    try:
        ping, online = kwargs["ping"], kwargs["online"]
        if ping < 0:
            errors["ping"] = {"error": "ping < 0"}
        if online not in (0, 1):
            errors["online"] = {"error": "online not in (0, 1)"}
    except Exception as e:
        errors["parse"] = str(e)

    if errors:
        await db.execute(f"""INSERT INTO public.error_status (occurred_at, object, errors_tuple, object_id) 
            VALUES ({time.time()}, '{kwargs["object"]}', '{json.dumps(errors)}', {kwargs["object_id"]});
        """)
    else:
        await db.execute(f"""INSERT INTO public.object_status (occurred_at, object, obj_id, online, ping) 
            VALUES ({time.time()}, '{kwargs["object"]}', {kwargs["object_id"]}, {online}, {ping});
        """)


async def check_token(token):
    if token != "super_secret_valid_token":
        raise ValueError("invalid token")


async def get_statuses(db, **kwargs):
    try:
        await check_token(str(kwargs["token"]))
        object_id = int(kwargs["object_id"])
        _object = str(kwargs.get("object", "server"))
    except BaseException:
        print("bad args")
        raise ValueError
    return [list(row) for row in await db.fetch(f"""SELECT occurred_at, online, ping, object, obj_id
        FROM object_status WHERE object = '{_object}' AND obj_id = '{object_id}'
        ORDER BY occurred_at
    """)]


async def get_statuses_errors_by_occurred_at(db, **kwargs):
    try:
        await check_token(str(kwargs["token"]))
        object_id = int(kwargs["object_id"])
        start_at = int(kwargs["start_at"])
        end_at = int(kwargs["end_at"])
        _object = str(kwargs.get("object", "server"))
        field = str(kwargs.get("field", "ping"))
    except BaseException:
        print("bad args")
        raise ValueError
    sql = f"""SELECT occurred_at, object, errors_tuple
        FROM error_status WHERE object_id = {object_id}
        AND occurred_at > {start_at} AND occurred_at < {end_at}
        AND errors_tuple ->> '{field}' != '' AND object = '{_object}'
        ORDER BY occurred_at
    """
    data = await db.fetch(sql)
    result = []
    for row in data:
        (occurred_at, _object, errors_tuple) = row
        errors = eval(errors_tuple)
        result += [{"object": _object, "occurred_at": occurred_at, "error": y["error"]} for x, y in errors.items()]
    return result

