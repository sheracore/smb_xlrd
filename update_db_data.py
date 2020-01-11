import psycopg2
import csv
import xlrd
import tempfile

from smb.SMBConnection import SMBConnection
from datetime import date, timedelta, datetime


class Mapping:

    def __init__(self):
        date_time = date.today() - timedelta(days=1)
        self.last_day = date_time.strftime('%Y-%m-%d') + ' 00:00:00'
        self.year = datetime.now().strftime("%Y")
        self.month = datetime.now().strftime("%B")[0:3]
        self.monthly = self.month + '-' + str(self.year[2:])

    def conn(self):
        self.potgres_conn = psycopg2.connect(
            database='rpat', user='postgres', host='X.X.X.X', password='Pass', port='5433')
        print('connected to DB')

    def get_newcell_from_db(self):
        cursor = self.potgres_conn.cursor()

        self.cell_list = []
        cursor.execute(
            "select distinct(t1.name) from cell_map as t1 inner join rnc_map as t2 on t1.bscid=t2.id where t2.type = 'HUAWEI_2G' and t1.deleted=false")
        cells = cursor.fetchall()
        for cell in cells:
            self.cell_list.append(cell[0])

    def add_to_mapping(self, files_list):
        cursor = self.potgres_conn.cursor()

        print(files_list)
        temp = '__sample__'

        for i in range(len(files_list)):
            cursor.execute("select max(id) from cell_map")
            id = cursor.fetchall()
            id = id[0][0]

            file_url = files_list[i]
            print('file_name -->', files_list[i])
            wb = xlrd.open_workbook(file_url)
            sheet_names = wb.sheet_names()
            for sheets in sheet_names:
                if sheets != 'GCELL':
                    continue
                print(sheets)
                sheet = wb.sheet_by_name(sheets)
                #values = list()

                for row_idx in range(2, sheet.nrows):
                    num_cols = sheet.ncols
                    value = tuple()
                    cell_name = sheet.cell_value(row_idx, 2)
                    bsc_name = sheet.cell_value(row_idx, 0)

                    if temp == cell_name:
                        continue

                    if cell_name in self.cell_list:
                        continue
                    temp = cell_name

                    cursor.execute(
                        "select t1.name from cell_map as t1 inner join rnc_map as t2 on t1.bscid=t2.id where t2.type = 'HUAWEI_2G' and t1.deleted=false and t1.name='%s'" % (cell_name))
                    cell_updated = cursor.fetchall()
                    if len(cell_updated) != 0:
                        continue
                    id += 1
                    print(cell_name)

                    cursor.execute(
                        "select id from rnc_map where name = '%s' " % (bsc_name))
                    bsc_id = cursor.fetchall()
                    print(bsc_id[0][0], cell_name, bsc_name)
                    if len(bsc_id) == 0:
                        continue

                    print("insert into cell_map(id,bscid,name,deleted) values({},{},'{}',false)".format(
                        id, bsc_id[0][0], cell_name))
                    cursor.execute("insert into cell_map(id,bscid,name,deleted) values({},{},'{}',false)".format(
                        id, bsc_id[0][0], cell_name))

            self.potgres_conn.commit()

    def get_from_file_sharing(self):
        userID = 'perfteam'
        password = 'pe123456rf'
        server_ip = '192.168.15.40'

        conn = SMBConnection(userID, password, 'machin_test',
                             'server_test', use_ntlm_v2=True, is_direct_tcp=True)
        assert conn.connect(server_ip, 139)

        f = conn.listPath('DriveTest-LogFile', '/Dump/%s/%s' %
                          (self.year, self.monthly))

        l = list()
        for row in f:
            if row.filename in ('.', '..'):
                continue
            l.append(int(row.filename[0:2]))
        day = max(l)
        if day < 10:
            day = '0' + str(day)
        week = str(day) + '-' + self.month
        print('month -->', self.month, 'week -->', week)

        f = conn.listPath('DriveTest-LogFile', '/Dump/%s/%s/%s/%s/%s/%s' %
                          (self.year, self.monthly, week, '2G', 'Huawei', 'Para'))
        count = 0
        files_list = list()
        for row in f:
            if row.filename in ('.', '..'):
                continue
            f = conn.listPath('DriveTest-LogFile', '/Dump/%s/%s/%s/%s/%s/%s/%s' %
                              (self.year, self.monthly, week, '2G', 'Huawei', 'Para', row.filename))
            for files in f:
                if files.filename in ('.', '..'):
                    continue
                file_name = '%s_%s.xlsm' % ('2G', count)
                file_obj = open(file_name, 'wb')
                s, f = conn.retrieveFile('DriveTest-LogFile', '/Dump/%s/%s/%s/%s/%s/%s/%s/%s' % (
                    self.year, self.monthly, week, '2G', 'Huawei', 'Para', row.filename, files.filename), file_obj)
                files_list.append(file_name)
                count += 1
                print(files.filename)
        self.add_to_mapping(files_list)


if __name__ == '__main__':

    obj = Mapping()
    obj.conn()
    obj.get_newcell_from_db()
    # obj.add_to_mapping()
    obj.get_from_file_sharing()
