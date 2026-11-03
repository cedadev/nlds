from nlds_processors.catalog.catalog import Catalog, CatalogError
from nlds_processors.catalog.catalog_models import Holding, File, Transaction
from nlds_processors.monitor.monitor import Monitor
from nlds.details import PathDetails, PathType

import nlds.server_config as CFG
import secrets
import string
import os.path
import random
import pwd
import grp

from uuid import uuid4
import time


def _get_or_create_transaction(
    catalog: Catalog, transaction_id: uuid4, holding: Holding
):
    # try to get the transaction to see if it already exists and can be
    # added to
    try:
        transaction = catalog.get_transaction(transaction_id=transaction_id)
    except (KeyError, CatalogError):
        transaction = None

    # create the transaction within the  holding if it doesn't exist
    if transaction is None:
        try:
            transaction = catalog.create_transaction(holding, transaction_id)
        except CatalogError as e:
            raise e
    return transaction


def _get_or_create_holding(
    catalog: Catalog, user: str, group: str, holding_name: str
) -> Holding:
    try:
        holding = catalog.get_holding(user, group, label=holding_name)
    except CatalogError:
        holding = catalog.create_holding(user, group, holding_name)
    return holding


def random_name(length=8):
    chars = string.ascii_letters + string.digits
    return "".join(secrets.choice(chars) for _ in range(length))


def create_file_path() -> str:
    # create a fakey file path
    GWS_BASE_PATH = "/Users/nrmassey/nlds-test-files"
    file_name = random_name()
    file_path = os.path.join(GWS_BASE_PATH, file_name)
    return file_path


def create_file_size() -> int:
    MIN_SIZE = 1024
    MAX_SIZE = 1024 * 1024
    size = random.randint(MIN_SIZE, MAX_SIZE)
    return size


def get_ids(user: str, group: str) -> tuple[int]:
    pwddata = pwd.getpwnam(user)
    pwd_uid = pwddata.pw_uid
    pwd_gid = pwddata.pw_gid
    gid = [
        g.gr_gid for g in grp.getgrall() if (user in g.gr_mem and group == g.gr_name)
    ]
    return pwd_uid, gid[0]


def _add_N_files(catalog: Catalog, user: str, group: str, holding_name: str, N: int):
    # Create a transaction in a holding and add N files to it
    holding = _get_or_create_holding(catalog, user, group, holding_name)
    transaction_id = str(uuid4())
    transaction = _get_or_create_transaction(catalog, transaction_id, holding)
    # Get the user id and group id
    uid, gid = get_ids(user, group)

    exists_time_sum = 0.0
    insert_time_sum = 0.0
    for i in range(0, N):
        original_path = create_file_path()
        size = create_file_size()
        permissions = 666
        time_1 = time.perf_counter()
        if catalog._file_exists_in_holding(
            user,
            group,
            holding_id=holding.id,
            original_path=original_path,
        ):
            raise CatalogError("File(s) already exists in holding")
        time_2 = time.perf_counter()
        exists_time_sum += time_2 - time_1
        # create the file
        time_1 = time.perf_counter()
        catalog.create_file(
            transaction,
            uid,
            gid,
            original_path,
            path_type=PathType.FILE,
            link_path=None,
            size=size,
            file_permissions=permissions,
        )
        time_2 = time.perf_counter()
        insert_time_sum += time_2 - time_1
    # commit after 1000 files
    catalog.commit()
    print(f"---- Exists time sum for {N} records: ", {exists_time_sum})
    print(f"---- Insert time sum for {N} records: ", {insert_time_sum})


def _add_N_filelist(catalog: Catalog, user: str, group: str, holding_name: str, N: int):
    # generate files in a filelist and add to the catalog
    filelist = []

    uid, gid = get_ids(user, group)

    for i in range(0, N):
        original_path = create_file_path()
        size = create_file_size()
        permissions = 666
        pd = PathDetails(
            user=uid,
            group=gid,
            original_path=original_path,
            path_type=PathType.FILE,
            link_path=None,
            size=size,
            permissions=permissions,
        )
        filelist.append(pd)

    exists_list = []
    holding = _get_or_create_holding(catalog, user, group, holding_name)

    transaction_id = str(uuid4())
    transaction = _get_or_create_transaction(
        catalog, transaction_id=transaction_id, holding=holding
    )
    catalog.session.flush()
    print("!!!", transaction.id)
    size = create_file_size()
    permissions = 666

    # make sure some files are added twice by adding them first
    for i in range(0, N >> 1):
        pd = filelist[i]
        catalog.create_file(
            transaction,
            user=pd.user,
            group=pd.group,
            original_path=pd.original_path,
            path_type=pd.path_type,
            link_path=pd.link_path,
            size=pd.size,
            file_permissions=pd.permissions,
        )

    time_1 = time.perf_counter()
    exists_list = catalog._filelist_exists_in_holding(
        user, group, holding_id=holding.id, filelist=filelist
    )
    add_list = list(set(filelist) - set(exists_list))
    for e in exists_list:
        x = filelist[filelist.index(e)]
        pd.failure_reason = "File already exists in holding"

    time_2 = time.perf_counter()
    exists_time_sum = time_2 - time_1
    print(f"---- Exists time sum for {N} records: ", {exists_time_sum})


def _count_files(catalog: Catalog, user: str, group: str, holding_name: str) -> int:
    holding = catalog.get_holding(user, group, label=holding_name)
    sum_files = 0
    for t in holding.transactions:
        sum_files += len(t.files)
    return sum_files


if __name__ == "__main__":

    config = CFG.load_config()
    db_engine = config["catalog_q"]["db_engine"]
    db_options = config["catalog_q"]["db_options"]

    user: str = "neil.massey"
    group: str = "perf_testing"
    holding_name: str = "Random3"

    catalog = Catalog(db_engine, db_options)
    catalog.connect()
    catalog.start_session()

    create_files = True
    count_files = not (create_files)

    if create_files:
        # _add_N_files(catalog, user, group, holding_name, 1000)
        _add_N_filelist(catalog, user, group, holding_name, 1000)
    elif count_files:
        n_files = _count_files(catalog, user, group, holding_name)
        print(f"Number of files in holding: {n_files}")
    catalog.end_session()
