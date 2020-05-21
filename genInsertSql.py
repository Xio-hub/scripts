import sys
import logging
import pymysql
import include.Common as Common


def get_customer_code():
    sql = "select customerCode,id from NewABSCustomerInfo"
    Common.DB.execute(sql)
    r = Common.DB.fetchall()
    return r


def generate_credit_control_info_sql(customer_codes):
    with open(Common.DATAPATH + 'CreditControlInfo.sql', 'w+', encoding='utf-8') as f:
        for customerCode in customer_codes:
            sql = "select * from NewABSCreditControlInfo where customerCode='%s' and book='NP'" % customerCode[0]
            Common.DB.execute(sql)
            res = Common.DB.fetchall()
            for r in res:
                r = list(r)
                if r and r[3] == 'NP':
                    if r[1] == None:
                        r[1] = customerCode[1]
                    values = (pymysql.escape_string(str(r[1])), pymysql.escape_string(str(r[2])), 'NMG',
                              pymysql.escape_string(str(r[4])), pymysql.escape_string(str(r[5])),
                              pymysql.escape_string(str(r[6])), pymysql.escape_string(str(r[7])),
                              pymysql.escape_string(str(r[8])), pymysql.escape_string(str(r[9])),
                              pymysql.escape_string(str(r[10])), Common.NowDateTime)
                    sql = "INSERT INTO NewABSCreditControlInfo(customerID,customerCode,book,approvedCreditLimit," \
                          "notificationLimit,autoLockLimit,approvedCreditTerms,practicalCreditTerms,basePrice,status," \
                          "addingDate) VALUES (%s);" % ', '.join(map(lambda x: "'" + '%s' + "'", values))
                    sql = sql % values
                    logging.info(sql)
                    # f.writelines(sql)
                    # f.write('\n')


def generate_billing_address_sql(customer_codes):
    with open(Common.DATAPATH + 'BillingAddress.sql', 'w+', encoding='utf-8') as f:
        for customerCode in customer_codes:
            sql = "select * from NewABSBillingAddress where customerCode='%s' and book='NP'" % customerCode[0]
            Common.DB.execute(sql)
            res = Common.DB.fetchall()
            for r in res:
                r = list(r)
                if r and r[3] == 'NP':
                    if r[1] == None:
                        r[1] = customerCode[1]
                    values = (pymysql.escape_string(str(r[1])), pymysql.escape_string(str(r[2])), 'NMG',
                              pymysql.escape_string(str(r[4])), pymysql.escape_string(str(r[5])),
                              pymysql.escape_string(str(r[6])), pymysql.escape_string(str(r[7])),
                              pymysql.escape_string(str(r[8])), pymysql.escape_string(str(r[9])),
                              pymysql.escape_string(str(r[10])), pymysql.escape_string(str(r[11])),
                              pymysql.escape_string(str(r[12])), Common.NowDateTime)
                    sql = "INSERT INTO NewABSBillingAddress(customerID,customerCode,book,billingName,forTheAttentionOf," \
                          "address1,address2,address3,address4,remark,defaultBillingAddress,addressPrintingOrder," \
                          "addingDate)VALUES (%s);" % ', '.join(
                        map(lambda x: "'" + '%s' + "'", values))
                    sql = sql % values
                    # logging.info(sql)
                    f.writelines(sql)
                    f.write('\n')


def generate_contact_sql(customer_codes):
    with open(Common.DATAPATH + 'Contact.sql', 'w+', encoding='utf-8') as f:
        for customerCode in customer_codes:
            sql = "select * from NewABSContact where customerCode='%s' and book='NP'" % customerCode[0]
            Common.DB.execute(sql)
            res = Common.DB.fetchall()
            for r in res:
                r = list(r)
                if r and r[3] == 'NP':
                    if r[1] == None:
                        r[1] = customerCode[1]
                    values = (pymysql.escape_string(str(r[1])), pymysql.escape_string(str(r[2])), 'NMG',
                              pymysql.escape_string(str(r[4])), pymysql.escape_string(str(r[5])),
                              pymysql.escape_string(str(r[6])), pymysql.escape_string(str(r[7])),
                              pymysql.escape_string(str(r[8])), pymysql.escape_string(str(r[9])),
                              Common.NowDateTime)

                    sql = "INSERT INTO NewABSContact(customerID,customerCode,book,contactName,titleOrDepartment," \
                          "telephone,fax,email,remark,addingDate) VALUES (%s);" % ', '.join(
                        map(lambda x: "'" + '%s' + "'", values))
                    sql = sql % values
                    # logging.info(sql)
                    f.writelines(sql)
                    f.write('\n')


def run():
    customerCodes = get_customer_code()
    generate_credit_control_info_sql(customerCodes)
    generate_billing_address_sql(customerCodes)
    generate_contact_sql(customerCodes)


if __name__ == "__main__":
    if len(sys.argv) > 1:
        Common.logInt(Common.LOGPATH + 'scripts_' + Common.NOW + '.log')
        logging.info('*' * 6 + ' script start ' + '*' * 6)
        # eval func
        funcs = sys.argv[1:]
        for func in funcs:
            eval(func)()
    else:
        sys.stderr.write(
            """Usage:  %s [Option]
Option
run
""" % sys.argv[0])
        sys.exit(1)

    Common.DB.close()
    logging.info('*' * 6 + ' script end ' + '*' * 6)
