from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.decorators import python_task, branch_task
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowFailException
import pandas as pd
import json
import psycopg2
import gspread
import requests


EMAIL = '<your email>'
PASSWORD = '<your_password>'
PATH = '<...>/dags/clan/'
csv_files = ['members', 'raids_attacks', 'raids', 'unattacked_players']
sheet_file_id = "<your_file_id>"

privilegies = {
    "admin": "Старейшина",
    "coLeader": "Соруководитель",
    "leader": "Глава",
    "member": "Участник"
}

default_args = {
    'depends_on_past': False,
    'owner': 'airflow',
    'start_date': datetime(2024, 12, 13)
}

clan_tag = '232RVCU8GQL'
token = "your token"
info_type = {
    "members": f"https://api.clashofclans.com/v1/clans/%{clan_tag}/members",
    "raids": f"https://api.clashofclans.com/v1/clans/%{clan_tag}/capitalraidseasons"
}


def get_last_raid_id(cursor):
    cursor.execute("SELECT last_value from raid_id;")
    return cursor.fetchall()[0][0]


def get_ip():
    ip_request = requests.get("https://ip.me/")
    return ip_request.text.replace("\n", "")


with DAG(
    "clash_api",
    max_active_runs=3,
    # we can change if can find active server that will work all the time
    schedule_interval=None,
    default_args=default_args,
    template_searchpath="<...>/dags/clan/",
    catchup=False
) as dag:

    connection = psycopg2.connect(database="clan", user="postgres",
                                  password="<...>", host="<...>", port=...)  # configure your connection
    cursor = connection.cursor()

    with open(PATH + 'scripts/creds.json') as f:
        credentials = json.load(f)
    gc = gspread.service_account_from_dict(credentials)

    task_start = DummyOperator(task_id="start")

    @python_task(task_id="close_connection", trigger_rule='none_failed')
    def end_operations(**context):
        connection.close()

    connection_end = end_operations()
    # Downloading json files
    with TaskGroup('loading_json_files') as loading_json_files:
        for (name, url) in info_type.items():
            bash_load = BashOperator(
                task_id=f"load_{name}",
                bash_command=(
                    f"curl -H 'Authorization: Bearer {token}' "
                    f"{url} > $AIRFLOW_HOME/dags/clan/json/{name}.json"
                )
            )

    # Creating TOKEN if needed and push it to XCom
    @python_task(task_id='create_token', provide_context=True, templates_dict={"path": PATH})
    def login_and_generate_a_token():
        s = requests.Session()

        log_in = {
            'email': EMAIL,
            'password': PASSWORD
        }
        # authorize to clash of clans api
        response = s.post(
            "https://developer.clashofclans.com/api/login",
            json=log_in
        ).json()
        if response['status']['message'] != "ok":
            raise AirflowFailException('Failed login: ' + response['status']['message'])

        # getting all keys
        ip_adress = get_ip()
        response = s.post('https://developer.clashofclans.com/api/apikey/list') \
                    .json()
        if response['status']['message'] != 'ok':
            raise AirflowFailException('Failed to get keys: ' + response['status']['message'])

        all_keys = response['keys']

        # if find ip match, return key, else delete it
        for key in all_keys:
            if ip_adress in key["cidrRanges"]:
                return key["key"]
            else:
                s.post(
                    'https://developer.clashofclans.com/api/apikey/revoke',
                    json={"id": key["id"]}
                )

        # creating new key for new current ip
        creating_key_payload = {
            'name': 'api from python',
            'description': 'created at ' + str(datetime.now()),
            'cidrRanges': ip_adress
        }
        response = s.post(
            'https://developer.clashofclans.com/api/apikey/create',
            json=creating_key_payload).json()
        if response['status']['message'] != 'ok':
            raise AirflowFailException('Failed to create a new key: ' + response['status']['message'])

        return response['key']['key']

    # Changing to Moscow timezone
    @python_task(task_id='cut_raid_json_part', templates_dict={"path": PATH})
    def cut_raid_json_part(templates_dict):
        with open(templates_dict['path']+'json/raids.json', 'r') as json_file:
            json_raid = json.load(json_file)['items'][0]
            start = json_raid['startTime'][:-1]
            end = json_raid['endTime'][:-1]
            s = (datetime.strptime(start, "%Y%m%dT%H%M%S.%f") +
                 timedelta(hours=3)).strftime("%Y%m%dT%H%M%S.%f")[:-3]
            e = (datetime.strptime(end, "%Y%m%dT%H%M%S.%f") +
                 timedelta(hours=3)).strftime("%Y%m%dT%H%M%S.%f")[:-3]
            json_raid['startTime'] = s
            json_raid['endTime'] = e
            del json_raid['attackLog']
            del json_raid['defenseLog']
        with open(templates_dict['path']+'json/raids.json', 'w') as json_file:
            json.dump(json_raid, json_file)

    # Updating clan's members info
    with TaskGroup('working_with_members') as working_with_members:
        @python_task(task_id='sql_members', templates_dict={"path": PATH})
        def create_sql_members(templates_dict):
            with open(templates_dict['path']+'sql/members.sql', 'w') as sql_file:
                sql_file.write("TRUNCATE TABLE members;\n")
                with open(templates_dict['path']+'json/members.json', 'r') as json_file:
                    members = json.load(json_file)['items']
                    for member in members:
                        sql_file.write("INSERT INTO members VALUES "
                                       f"('{member['tag']}', '{member['name']}', '{privilegies[member['role']]}', {member['trophies']});\n")

        @python_task(task_id="upload_members_to_google_drive")
        def upload_members_to_drive():
            name = 'members'
            row = 2

            print("[uploading to google drive]")
            sh = gc.open_by_key(sheet_file_id)
            sheet = sh.worksheet(name)
            cursor.execute(f"SELECT * from {name};")
            sheet.batch_clear([f"A{row}:D"])
            sql_info = cursor.fetchall()
            sheet.append_rows(sql_info)

            print(f"{name} was successfully uploaded")

        c_sql_members = create_sql_members()
        upload_members = upload_members_to_drive()

        members_update = PostgresOperator(
            postgres_conn_id="clan_server",
            task_id="updating_members",
            sql="sql/members.sql"
        )

        c_sql_members >> members_update >> upload_members

    # Updating clan's raids info
    with TaskGroup('working_with_raids', prefix_group_id=False) as working_with_raids:
        @branch_task(task_id='checking_for_raid_updates', templates_dict={"path": PATH})
        def check_for_updates(templates_dict):
            cursor.execute("SELECT last_value from raid_id;")
            last_raid_id = cursor.fetchall()[0][0]

            cursor.execute(
                f"SELECT * from raids where raid_id = {last_raid_id};")
            raid_info = cursor.fetchall()

            timestamp_now = datetime.now()
            print(timestamp_now)
            if raid_info == []:
                return "add_raid_info"
            if timestamp_now <= raid_info[0][3]:
                return "update_fresh_raid_info"
            elif raid_info[0][3] < timestamp_now:
                start = ''
                end = ''
                str_timestamp = timestamp_now.strftime("%Y%m%dT%H%M%S.%f")[:-3]
                with open(templates_dict['path']+'json/raids.json', 'r') as json_file:
                    json_raid = json.load(json_file)
                    start = json_raid['startTime']
                    end = json_raid['endTime']
                if start <= str_timestamp < end:
                    return "add_raid_info"
                else:
                    return "end_current_raid_if_need"

        @python_task(task_id="add_raid_info", templates_dict={"path": PATH})
        def add_raid_row(templates_dict):
            with open(templates_dict['path']+'json/raids.json', 'r') as json_file:
                raid = json.load(json_file)
                timestamp_now = datetime.now().strftime(
                    "%Y%m%dT%H%M%S.%f")[:-7]
                cursor.execute(
                    "INSERT INTO raids VALUES "
                    f"(nextval('raid_id'), '{raid['state']}', '{raid['startTime']}', '{raid['endTime']}', "
                    f"{raid['capitalTotalLoot']}, {raid['raidsCompleted']}, {raid['totalAttacks']}, {raid['enemyDistrictsDestroyed']}, "
                    f"'{timestamp_now}');"
                )
                connection.commit()

        @python_task(task_id="update_fresh_raid_info", templates_dict={"path": PATH})
        def update_fresh_raid_info(templates_dict):
            cursor.execute("SELECT last_value from raid_id;")
            last_raid_id = cursor.fetchall()[0][0]

            with open(templates_dict['path']+'json/raids.json', 'r') as json_file:
                raid = json.load(json_file)
                timestamp_now = datetime.now().strftime(
                    "%Y%m%dT%H%M%S.%f")[:-7]
                cursor.execute(
                    f"UPDATE raids set capitaltotalloot={raid['capitalTotalLoot']}, "
                    f"raidscompleted = {raid['raidsCompleted']}, "
                    f"totalattacks = {raid['totalAttacks']}, "
                    f"districtdestroyed = {raid['enemyDistrictsDestroyed']}, "
                    f"lastupdated = '{timestamp_now}' "
                    f"where raid_id = {last_raid_id};"
                )
                connection.commit()

        @branch_task(task_id="end_current_raid_if_need", templates_dict={"path": PATH})
        def end_raid_if_needed(templates_dict):
            last_raid_id = get_last_raid_id(cursor)
            cursor.execute(
                f"SELECT * from raids where raid_id = {last_raid_id};")
            raid_info = cursor.fetchall()[0]
            timestamp_now = datetime.now().strftime("%Y%m%dT%H%M%S.%f")[:-7]
            if (raid_info[3] > raid_info[8]):
                with open(templates_dict['path']+'json/raids.json', 'r') as json_file:
                    raid = json.load(json_file)
                    cursor.execute(
                        f"UPDATE raids set state='ended', "
                        f"starttime = '{raid['startTime']}', "
                        f"endtime = '{raid['endTime']}', "
                        f"capitaltotalloot = {raid['capitalTotalLoot']}, "
                        f"raidscompleted = {raid['raidsCompleted']}, "
                        f"totalattacks = {raid['totalAttacks']}, "
                        f"districtdestroyed = {raid['enemyDistrictsDestroyed']}, "
                        f"lastupdated = '{timestamp_now}' "
                        f"where raid_id = {last_raid_id};"
                    )
                    connection.commit()
                print("[Getting last raid attacks]")
                return "create_sql_raids"
            cursor.execute(
                f"UPDATE raids set lastupdated = '{timestamp_now}' "
                f"where raid_id = {get_last_raid_id(cursor)};"
            )
            connection.commit()
            return "close_connection"

        @python_task(task_id="create_sql_raids", templates_dict={"path": PATH}, trigger_rule="none_failed_min_one_success")
        def create_sql_raids(templates_dict):
            with open(templates_dict['path']+'sql/raids.sql', 'w') as sql_file:
                with open(templates_dict['path']+'json/raids.json', 'r') as json_file:
                    raid_info = json.load(json_file)
                    members = raid_info['members']
                    cursor.execute("SELECT last_value from raid_id;")
                    last_raid_id = int(cursor.fetchall()[0][0])
                    sql_file.write(
                        f"DELETE FROM raids_attacks where raid_id = {last_raid_id};\n")
                    for member in members:
                        sql_file.write(
                            "INSERT INTO raids_attacks VALUES ("
                            f"{last_raid_id}, '{member['tag']}', {member['attacks']}, "
                            f"{member['bonusAttackLimit']}, {member['capitalResourcesLooted']});\n"
                        )

        @python_task(task_id="upload_raid_to_google_drive")
        def upload_raid_to_drive():
            headers = {
                'raids': ['raid_id', 'state', 'startTime', 'endTime', 'capitalTotalLoot', 'raidsCompleted', 'totalAttacks', 'districtDestroyed', 'lastupdated']
            }
            files_params = {
                'raids_attacks': ['E', []],
                'raids': ['I', ["startTime", "endTime", "lastupdated"]],
                'unattacked_players': ['B', []]
            }

            print("[uploading to google drive]")
            for name, params in files_params.items():
                sh = gc.open_by_key(sheet_file_id)
                sheet = sh.worksheet(name)
                last_raid_id = get_last_raid_id(cursor)
                if row := sheet.find(str(last_raid_id), in_column=1):
                    sheet.batch_clear([f"A{row.row}:{params[0]}"])

                cursor.execute(
                    f"SELECT * from {name} where raid_id = {last_raid_id};")
                sql_info = cursor.fetchall()

                if cols := params[1]:
                    df = pd.DataFrame(sql_info, columns=headers[name])
                    df = df.astype(dict([(col, "str") for col in cols]))
                    sql_info = df.to_numpy().tolist()

                sheet.append_rows(sql_info)
                print(f"{name} was successfully uploaded")

        check_f_updates = check_for_updates()
        add_and_update_raid = add_raid_row()
        update_raid_info = update_fresh_raid_info()
        end_raid_if_need = end_raid_if_needed()
        c_sql_raids = create_sql_raids()
        upload_raids = upload_raid_to_drive()

        updating_raid_attacks = PostgresOperator(
            postgres_conn_id="clan_server",
            task_id="updating_raid_attacks",
            sql="sql/raids.sql"
        )

        unattacked_update = PostgresOperator(
            postgres_conn_id="clan_server",
            task_id="updating_unattacked_players",
            sql='''
                DELETE FROM unattacked_players WHERE raid_id = {{ params.last_raid_id }};
                INSERT INTO unattacked_players (
                    SELECT {{ params.last_raid_id }}, m.tag FROM members AS m
                    EXCEPT
                    SELECT raid_id, tag from raids_attacks where raid_id = {{ params.last_raid_id }} 
                )
            ''',
            params={"last_raid_id": get_last_raid_id(cursor)}
        )

        check_f_updates >> [add_and_update_raid,
                            update_raid_info, end_raid_if_need]
        end_raid_if_need >> connection_end
        [add_and_update_raid, update_raid_info, end_raid_if_need] >> c_sql_raids
        c_sql_raids >> updating_raid_attacks >> unattacked_update >> upload_raids

    create_token = login_and_generate_a_token()
    cut_raid_json = cut_raid_json_part()

    task_start >> create_token
    create_token >> loading_json_files
    loading_json_files >> cut_raid_json
    cut_raid_json >> working_with_members
    cut_raid_json >> working_with_raids
    [working_with_members, working_with_raids] >> connection_end
