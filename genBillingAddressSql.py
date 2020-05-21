import pymysql
import logging
import include.Common as Common

sql = "select customerCode from NewABSCustomerInfo"
Common.DB.execute(sql)
customerCodes = Common.DB.fetchall()
with open(Common.DATAPATH + 'BillingAddress.sql', 'w+', encoding='utf-8') as f:
    for customerCode in customerCodes:
        sql = "select * from NewABSBillingAddress where customerCode='%s' and book='NP'" % customerCode[0]
        Common.DB.execute(sql)
        res = Common.DB.fetchall()
        for r in res:
            if r and r[3] == 'NP':
                values = (pymysql.escape_string(str(r[1])), pymysql.escape_string(str(r[2])), 'NMG',
                          pymysql.escape_string(str(r[4])), pymysql.escape_string(str(r[5])),
                          pymysql.escape_string(str(r[6])), pymysql.escape_string(str(r[7])),
                          pymysql.escape_string(str(r[8])), pymysql.escape_string(str(r[9])),
                          pymysql.escape_string(str(r[10])), pymysql.escape_string(str(r[11])),
                          pymysql.escape_string(str(r[12])), Common.NowDateTime)
                sql = "INSERT INTO NewABSBillingAddress(customerID,customerCode,book,billingName,forTheAttentionOf,address1,address2,address3,address4,remark,defaultBillingAddress,addressPrintingOrder,addingDate)VALUES (%s);" % ', '.join(map(lambda x: "'"+'%s'+"'", values))
                sql = sql % values
                # logging.info(sql)
                f.writelines(sql)
                f.write('\n')

