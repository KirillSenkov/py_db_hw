import psycopg2


def create_structures(conn: psycopg2.extensions.connection):
    with conn.cursor() as cur:
        cur.execute('''
        CREATE TABLE IF NOT EXISTS customers (cus_id SERIAL PRIMARY KEY,
                                              name VARCHAR(50) NOT NULL,
                                              surname VARCHAR(100) NOT NULL,
                                              e_mail VARCHAR(100) NOT NULL,
                                              UNIQUE (name, surname, e_mail)
                                              );
        CREATE INDEX IF NOT EXISTS IDX_CUS_NAME ON CUSTOMERS(name);
        CREATE INDEX IF NOT EXISTS IDX_CUS_SURNAME ON CUSTOMERS(surname);
        CREATE INDEX IF NOT EXISTS IDX_CUS_EMAIL ON CUSTOMERS(e_mail);
        CREATE TABLE IF NOT EXISTS phones (cus_id INTEGER REFERENCES 
                                                      customers(cus_id),
                                           phone_id SERIAL PRIMARY KEY,
                                           phone_num VARCHAR(100) NOT NULL,
                                           UNIQUE (cus_id, phone_num)
                                           );
        CREATE INDEX IF NOT EXISTS IDX_PHONE_NUM ON PHONES(phone_num);
                    ''')
        conn.commit()
        print('Structuses created successfully.')


def add_phone(conn: psycopg2.extensions.connection,
              cus_id: int,
              phone_num: str):
    with conn.cursor() as cur:
        try:
            cur.execute('''
                INSERT INTO phones (cus_id, phone_num) values(%s, %s)
                        ''', (cus_id, phone_num))
            conn.commit()
            print('Customer phone number added successfully.')
        except psycopg2.errors.ForeignKeyViolation:
            conn.rollback()
            print(f'There is no customer with ID = {cus_id}.')
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            print('The phone number is added for the customer allready.')


def add_cus(conn: psycopg2.extensions.connection,
            name: str,
            surname: str,
            e_mail: str,
            phones: list):
    with conn.cursor() as cur:
        cur.execute('''
        SELECT * FROM customers WHERE name = %s
                                       AND surname = %s
                                       AND e_mail = %s
                 ''', (name, surname, e_mail)
                    )

        if len(cur.fetchall()) == 0:
            cur.execute('''
            INSERT INTO customers (name, surname, e_mail)
            VALUES (%s, %s, %s)
                     ''', (name, surname, e_mail)
                        )
            print('Customer added successfully.')
            cur.execute('''
            SELECT cus_id FROM customers WHERE name = %s
                                           AND surname = %s
                                           AND e_mail = %s
                                 ''', (name, surname, e_mail))
            cus_id = cur.fetchall()[0]

            for phone in phones:
                add_phone(conn, cus_id, phone)

            conn.commit()

        else:
            print("There's allready exists a person with "
                  "the same name, surname and e_mail. It's an evel tween?")


def change_cus(conn: psycopg2.extensions.connection,
               cus_id: int,
               name: str=None,
               surname: str=None,
               e_mail: str=None,
               phones: list=[]):
    with conn.cursor() as cur:
        cur.execute('''
                  SELECT * FROM customers WHERE cus_id = %s
                          ''', (cus_id, ))
        cus = cur.fetchone()
        if cus is None:
            print(f"There's no person with ID = {cus_id}'")
            return

        if name is None:
            name = cus[1]
        if surname is None:
            surname = cus[2]
        if e_mail is None:
            e_mail = cus[3]
        cur.execute('''
          UPDATE customers SET
            name = %s,
            surname = %s,
            e_mail = %s
          WHERE cus_id = %s
          ''', (name, surname, e_mail, cus_id))
        if phones is None or phones == []:
            pass
        else:
            cur.execute('''
                        DELETE FROM phones WHERE cus_id = %s
                        ''', (cus_id, ))
            for phone in phones:
                add_phone(conn, cus_id, phone)
        conn.commit()
        print('Customer data changed.')


def delete_phone(conn: psycopg2.extensions.connection,
                 cus_id: int,
                 phone: str):
    with conn.cursor() as cur:
        cur.execute('''
        SELECT * FROM customers c
            LEFT JOIN phones p
            ON c.cus_id = p.cus_id AND phone_num = %s
        WHERE
          c.cus_id = %s
                    ''', (phone, cus_id))
        cus_phone = cur.fetchone()
        if cus_phone is None:
            print(f'There is no customer with ID = {cus_id}.')
        elif cus_phone[4] is None:
            print(f'There is no such phone number as "{phone}" of customer '
                  f'with ID = {cus_id}.')
        else:
            cur.execute('''
                        DELETE FROM phones WHERE cus_id = %s AND phone_num = %s
                        ''', (cus_id, phone))
            conn.commit()
            print('Phone number deleted successfully.')


def delete_customer(conn: psycopg2.extensions.connection, cus_id: int):
    with conn.cursor() as cur:
        cur.execute('''
        SELECT cus_id FROM customers WHERE cus_id = %s
                    ''', (cus_id, ))
        cusid = cur.fetchone()
        if cusid is None:
            print(f'There is no customer with ID = {cus_id}.')
        else:
            cur.execute('''
            DELETE FROM phones WHERE cus_id = %s;
            DELETE FROM customers WHERE cus_id = %s;''', (cus_id, cus_id))
            conn.commit()
            print('Customer deleted successfully.')


def find_customer(conn: psycopg2.extensions.connection,
                  name: str=None,
                  surname: str=None,
                  e_mail: str=None,
                  phone: str =None):
    with conn.cursor() as cur:
        cur.execute('''
        WITH t_args AS (SELECT %s AS name,
                               %s AS surname,
                               %s AS e_mail,
                               %s AS phone)
        SELECT c.* FROM t_args t, customers c
        WHERE 
              c.name    = COALESCE(t.name, c.name)
          AND c.surname = COALESCE(t.surname, c.surname)
          AND c.e_mail  = COALESCE(t.e_mail, c.e_mail)
          AND (t.phone IS NULL
               OR EXISTS(SELECT 1 FROM phones p
                         WHERE p.cus_id    = c.cus_id
                           AND p.phone_num = t.phone
                         )
               )
                    ''', (name, surname, e_mail, phone))
        res = cur.fetchall()
        if len(res) == 0:
            print('No customers found.')
        else:
            print(f'{len(res)} customers found:')
            for id, name, surname, e_mail in res:
                print(f'ID {id}: {name} {surname}, e-mail: {e_mail}')


with psycopg2.connect(database='netology_db',
                      user='postgres',
                      password='postgres'
                      ) as conn:

    print('#1 Creating structures.===========================================')
    create_structures(conn)

    print('#2 Customers adding.==============================================')
    add_cus(conn, 'John', 'Johnson', 'a@b.c', ['+7-953-405-76-45', '3324562'])
    add_cus(conn, 'John', 'Johnson', 'a@b.c', ['+7-953-405-76-45', '3324562'])
    add_cus(conn, 'Jane', 'Jackson', 'd@e.f', ['+7-952-305-37-88', '3325739'])
    add_cus(conn, 'Jane', 'Robinson', 'g@h.i', ['3329236'])

    print('#3 Phone number adding.===========================================')
    add_phone(conn, 10, '+7-953-768-64-11')
    add_phone(conn, 1, '+7-953-405-76-45')
    add_phone(conn, 1, '+7-953-768-64-11')

    print('#4 Customer changing.=============================================')
    change_cus(conn, cus_id=5)
    change_cus(conn, cus_id=2, surname='Johnson', phones=['+7-952-305-37-88',
                                                          '3324562'])

    print('#5 Phone deleting.================================================')
    delete_phone(conn, cus_id=10, phone='')
    delete_phone(conn, cus_id=1, phone='123')
    delete_phone(conn, cus_id=1, phone='+7-953-405-76-45')

    print('#6 Customer deleting.=============================================')
    delete_customer(conn, 10)
    delete_customer(conn, 1)

    print('#7 Customer searching.============================================')
    find_customer(conn, name='Sigizmund', surname='Zilberstein')
    find_customer(conn, name='Jane')
    find_customer(conn, name='Jane', phone='+7-952-305-37-88')
