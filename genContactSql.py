import logging
import pymysql
import include.Common as Common

sql = "select customerCode from NewABSCustomerInfo"
Common.DB.execute(sql)
customerCodes = Common.DB.fetchall()
with open(Common.DATAPATH + 'Contact.sql', 'w+', encoding='utf-8') as f:
    for customerCode in customerCodes:
        sql = "select * from NewABSContact where customerCode='%s' and book='NP'" % customerCode[0]
        Common.DB.execute(sql)
        res = Common.DB.fetchall()
        for r in res:
            if r and r[3] == 'NP':
                values = (pymysql.escape_string(str(r[1])), pymysql.escape_string(str(r[2])), 'NMG',
                          pymysql.escape_string(str(r[4])), pymysql.escape_string(str(r[5])),
                          pymysql.escape_string(str(r[6])), pymysql.escape_string(str(r[7])),
                          pymysql.escape_string(str(r[8])),pymysql.escape_string(str(r[9])),
                          Common.NowDateTime)
                sql = "INSERT INTO NewABSContact(customerID,customerCode,book,contactName,titleOrDepartment,telephone,fax,email,remark,addingDate) VALUES (%s);" % ', '.join(map(lambda x: "'"+'%s'+"'", values))
                sql = sql % values
                f.writelines(sql)
                f.write('\n')
