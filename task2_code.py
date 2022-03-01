import json
import time

"""
Общий комментарий 1: данные функции реализуют совершенно определенный функционал, в данном случае - получения и добавления
метрик, поэтому можно было бы объединить их в класс, например MetricsData. Там эти функции сделать методами, 
это могло бы выглядеть лаконичнее.
Но в целом это вкусовщина, делать так не обязательно, можно вполне оставить как есть - набором независимых функций.

Общий комментарий 2: во всех функциях нет typing-ов у аргументов и возвращаемых значений. 
Понятно, что это тоже не обязательно, но хорошим тоном было бы эти typing добавить.

Общий комментарий 3: почти везде используются **kwargs, а потом берутся совершенно конкретные ключи в kwargs. Почему бы
не оформить их в аргументы функции? Так было бы понятнее, нагляднее, им можно было бы привязать типы данных, проверить 
наличие аргументов без try-except. Я бы переписал все в более явном виде.

Общий комментарий 4: нет логгера и соответственно логов. А хотелось бы :)

"""


async def create_tables(db):
    # Иногда таблички создают в инициализирующей миграции.
    # Но если предусмотрено импортировать эти функции в миграцию отсюда, то это ок.
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
    # Кажется, можно и тут проверить токен
    errors = {}
    ping, online = None, None
    # Действительно ли нужно объявлять эти переменные здесь? Если воспользоваться методом kwargs.get(),
    # в случае отсутствия такого ключа мы получили бы None. Хотя по логике кода, мы все равно не окажемся в той ветке,
    # где эти переменные участвуют в запросе, в случае, когда их не передано.

    try:
        ping, online = kwargs["ping"], kwargs["online"]

        # Тут в kwargs может не быть нужного нам аргумента, тогда мы ошибку запишем в базу с текстом "KeyError", что
        # может быть не вполне очевидным.
        # А вообще, раз мы здесь и далее ожидаем в kwargs вполне конкретные ключи,
        # то лучше явно их задать либо в неком объекте, который бы приходил как аргумент, либо вообще необязательными
        # аргументами в функции

        if ping < 0:
            errors["ping"] = {"error": "ping < 0"}
        if online not in (0, 1):
            errors["online"] = {"error": "online not in (0, 1)"}
    except Exception as e:
        # Возможно лучше здесь обработать тут KeyError отдельно и дать более понятное описание ошибки
        errors["parse"] = str(e)

    if errors:

        # возможно, я не совсем верно понимаю цель и предназначение сервиса, но должны ли мы сохранять ошибку
        # обработки входных данных в таблицу ошибок?
        # Изначально мне казалось, что таблица ошибок должна содержать ошибки получения метрик,
        # а мы докинем туда ошибки некорретных запросов.
        # поэтому я считаю, что KeyError надо точно обработать отдельно.

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
    # я бы вынес это во внешнюю зависимость (в middleware, например, или в FastAPI-шную dependency),
    # и проверял бы токен где-то снаружи


async def get_statuses(db, **kwargs):
    try:
        await check_token(str(kwargs["token"]))
        object_id = int(kwargs["object_id"])
        _object = str(kwargs.get("object", "server"))
    except BaseException:
        # Ну слишком широкий exception, можно было бы оставить вовсе просто except
        print("bad args")  # как потом мы найдем этот принт, если, например, наш код задеплоен в облаке?
        # И что он вообще полезного нам скажет? Нужен логгер, принты - плохо
        raise ValueError  # Совершенно любой эксепшн станет здесь ValueError, может лучше написать просто raise?
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
    # А почему в этой функции отдельно вынесен Raw-SQL код в переменную, а в функциях выше - нет?
    # Лучше сделать единообразно во всех функциях.
    data = await db.fetch(sql)
    result = []
    for row in data:
        (occurred_at, _object, errors_tuple) = row
        errors = eval(errors_tuple)
        result += [{"object": _object, "occurred_at": occurred_at, "error": y["error"]} for x, y in errors.items()]
    return result

