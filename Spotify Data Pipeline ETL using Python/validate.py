import snowflake.connector
import usf_cipher
import csv
import re
from tqdm import tqdm
import sys

import custom_rules


OUT_HEADERS = ["rule_num", "validation_rule", "table_name", "column_validated", "key", "message"]

def get_creds(key_f, pwd_f):
    try:
        creds = usf_cipher.decrypt(key_f, pwd_f)
        json_creds = dict(eval(creds.strip()))
        return json_creds
    except Exception as e:
        print("Failed to read Snowflake credentials: %s" % str(e))
        return None

def get_conn(warehouse, database, schema, role):
    try:
        key_f = "c:\\apps\\usf_config\\snowflake_dev_creds.key"
        pwd_f = "c:\\apps\\usf_config\\snowflake_dev_creds.pwd"
        creds = get_creds(key_f, pwd_f)
        username = creds.get("username")
        pwd = creds.get("password")
        account = creds.get("account")
        conn = snowflake.connector.connect(
            user=username,
            password=pwd,
            account=account,
        )
        conn.cursor().execute("USE ROLE " + role)
        conn.cursor().execute("USE WAREHOUSE " + warehouse)
        conn.cursor().execute("USE DATABASE " + database)
        conn.cursor().execute("USE SCHEMA " + schema)
        return conn
    except Exception as e:
        print("Failed to connect to Snowflake: %s" % str(e))
        return None

def output_report(results):
    try:
        with open("invalid.csv", "w", encoding="utf-8", newline="") as f:
            dict_writer = csv.DictWriter(f, OUT_HEADERS)
            dict_writer.writeheader()
            dict_writer.writerows(results)
    except Exception as e:
        print("Failed to output CSV report %s" % str(e))

def output_to_snowflake(results):
    conn = get_conn("DEV_QA_WH", "USF_INTEGRATIONS", "RAW_INTEGRATIONS", "INTEGRATIONS_RO_ROLE")
    if not len(results):
        return
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE IF EXISTS MDM_DQ_VALIDATION_OUTPUT")
    q = "INSERT INTO MDM_DQ_VALIDATION_OUTPUT ("
    q += OUT_HEADERS[0]
    for i in range(1, len(OUT_HEADERS)):
        q += ","
        q += OUT_HEADERS[i]
    q += ") VALUES \n"

    def format_result(result):
        result_q = "('%s', '%s', '%s', '%s', '%s', '%s')" % (
            result.get("rule_num").replace("'", ""),
            result.get("validation_rule").replace("'", ""),
            result.get("table_name").replace("'", ""),
            result.get("column_validated").replace("'", ""),
            result.get("key").replace("'", ""),
            result.get("message").replace("'", "")
        )
        return result_q

    q += format_result(results[0])
    insert_count = 1
    for i in range(1, len(results)):
        if insert_count >= 10000:
            cur.execute(q)
            q = "INSERT INTO MDM_DQ_VALIDATION_OUTPUT ("
            q += OUT_HEADERS[0]
            for i in range(1, len(OUT_HEADERS)):
                q += ","
                q += OUT_HEADERS[i]
            q += ") VALUES \n"
            q += format_result(results[i])
            insert_count = 1
            continue
        q += ",\n"
        q += format_result(results[i])
        insert_count += 1
    cur.execute(q)

def is_valid(value_to_validate, regex, is_count, count_min=None, count_max=None, is_negated=False):
    if value_to_validate is None:
        return True
    is_good = None
    if is_count and count_min is not None and count_max is not None:
        search = re.findall(regex, value_to_validate)
        is_good = count_min <= len(search) <= count_max
    else:
        is_good = bool(re.fullmatch(regex, value_to_validate))
    return not is_good if is_negated else is_good

def read_rules():
    rules = None
    try:
        with open("rules.csv") as f:
            rules = [{k: str(v) for k, v in row.items()}
                for row in csv.DictReader(f, skipinitialspace=True)]
        return rules
    except Exception as e:
        print("Failed to read rules %s: " % str(e))
    finally:
        return rules

def main():
    start = 1
    if len(sys.argv) == 2:
        start = int(sys.argv[1])
    # Establish Snowflake connection
    conn = get_conn("DEV_MVP_WH", "USF_DERIVED_ZONE", "MDM_GOLDEN", "DZ_READ_ONLY")
    cur = conn.cursor(snowflake.connector.DictCursor)

    # Read rules CSV file
    rules = read_rules()

    # Apply rules to each table in Snowflake
    invalid = []
    print("Appying rules...\n")
    for rule in rules:
        rule_num = rule.get("rule_num")
        # if start provided, then skip
        if int(rule_num[2:]) < start:
            continue
        rule_name = rule.get("rule_name")
        table_name = rule.get("table_name")
        column_to_validate = rule.get("column_to_validate")
        regex = r"" + rule.get("regex")
        is_count = bool(int(rule.get("is_count")))
        count_min = None if not is_count else int(rule.get("count_min"))
        count_max = None if not is_count else int(rule.get("count_max"))
        is_negated = False if int(rule.get("is_negated")) == 0 else True

        output = "%s on %s.%s" % (rule_name, table_name, column_to_validate)
        print(output)
        
        # custom rule
        if len(regex) == 0:
            custom_rule_func = getattr(custom_rules, rule_num)
            invalid.extend(custom_rule_func(conn, rule))
            continue

        # Perform validation on the given table
        cur.execute("SELECT * FROM %s" % table_name)
        records = cur.fetchall()
        for i in tqdm(range(len(records))):
            d = records[i]
            value_to_validate = d.get(column_to_validate)
            if not is_valid(value_to_validate, regex, is_count, count_min, count_max, is_negated):
                value_to_validate = value_to_validate.replace("'", "") # replace single quotes to avoid SQL error
                key = d.get("SOURCE_PRIMARY_KEY")
                invalid_message = '"%s" is not validly formatted' % value_to_validate
                invalid.append({
                    "rule_num": rule_num,
                    "validation_rule": rule_name,
                    "table_name": table_name,
                    "column_validated": column_to_validate,
                    "key": key,
                    "message": invalid_message
                })

    output_report(invalid)
    print("Outputting to Snowflake...")
    output_to_snowflake(invalid)
    print("Done!")


if __name__ == "__main__":
    main()