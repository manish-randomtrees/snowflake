import oracledb
from environment import *
oracle_connection = oracledb.connect(
                                        user=ORACLE_USER,
                                        password=ORACLE_PWD,
                                        dsn=ORACLE_DSN
                                        )

ora_cursor = oracle_connection.cursor()
#to save procedure from oracle to file
for j in ['EXPORT_ORACLE_SF_FK_METADATA','EXPORT_ORACLE_SF_METADATA','EXPORT_ORACLE_SF_PK_METADATA','EXPORT_ORACLE_SF_UK_METADATA','UPDATE_METADATA_INFO_TO_ZERO','WRAPPER_GENERATE_FK_METADATA','WRAPPER_GENERATE_METADATA','WRAPPER_GENERATE_PK_METADATA','WRAPPER_GENERATE_UK_METADATA']:
    truncate_table = f"""select text from dba_source where owner='C##HR' and name='{j}'"""
    ora_cursor.execute(truncate_table)
    # file = open('pro.sql','wb')
    with open(f'{j}.sql','a') as file:
        for i in ora_cursor.fetchall():
            file.write(i[0])
#getting table ddl from oracle
ora_cursor.execute("select dbms_metadata.get_ddl('TABLE','SF_UK_EXPORT_METADATA') from dual")
for i  in ora_cursor.fetchall():
    print(i[0].read())
