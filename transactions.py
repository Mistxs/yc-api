import requests
import sqlite3
from datetime import datetime
start = datetime.now()

headers = {
    'Accept': 'application/vnd.yclients.v2+json',
    'Content-Type': 'application/json',
    'Authorization': ''

}

start_date = '2023-08-01'
end_date = '2023-08-31'


def create_connection():
    conn = None
    try:
        conn = sqlite3.connect('transactions.db')
    except sqlite3.Error as e:
        print(e)

    return conn

def collect_staff(salon_id, fired=False, deleted=False):
    url = f"https://api.yclients.com/api/v1/company/{salon_id}/staff/"
    response = requests.request("GET", url, headers=headers).json()
    fired_staff = []
    deleted_staff = []
    all_staff = []

    for master in response["data"]:
        if fired and master["is_fired"]:
            fired_staff.append(master["id"])
        if deleted and master["is_deleted"]:
            deleted_staff.append(master["id"])
        if not master.get("is_deleted") and not master.get("is_fired"):
            all_staff.append(master["id"])


    if fired and deleted:
        return fired_staff, deleted_staff
    elif fired:
        return fired_staff
    elif deleted:
        return deleted_staff
    else:
        return all_staff

def collect_transactions(salon_id,start_date,end_date, masters):
    conn = create_connection()
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS transactions
                        (transaction_id INT PRIMARY KEY NOT NULL,
                         master_id INT,
                         transaction_type INT,
                         transaction_desc TEXT,
                         amount FLOAT);''')

    url = f"https://api.yclients.com/api/v1/transactions/{salon_id}?start_date={start_date}&end_date={end_date}&master_id={masters}&count=1000"
    response = requests.request("GET", url, headers=headers).json()
    for item in response["data"]:
        transaction_id = item["id"]
        master_id = masters
        transaction_type = int(item["expense"]["id"])
        transaction_desc = item["expense"]["title"]
        amount = item["amount"]

        cursor.execute("INSERT  OR IGNORE  INTO transactions (transaction_id, master_id, transaction_type, transaction_desc, amount) VALUES (?, ?, ?, ?, ?)",
                   (transaction_id, master_id, transaction_type, transaction_desc, amount))

    # Сохранение изменений и закрытие соединения
    conn.commit()
    conn.close()

def display_transactions(masters):
    conn = create_connection()
    cursor = conn.cursor()
    masters_str = ','.join(str(master) for master in masters)

    cursor.execute(f'''select master_id,transaction_desc,sum(amount) 
                        from transactions
                        where master_id in ({masters_str})
                        group by master_id, transaction_desc''')

    for row in cursor.fetchall():
        print(row)

    conn.close()



masters = (collect_staff(41120,fired=False, deleted=False))

for master in masters:
    collect_transactions(41120,start_date,end_date,master)

display_transactions(masters)



end = datetime.now()
queryts = end-start
print(f"query is running: {queryts}")



