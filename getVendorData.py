import sys
import logging
import include.Common as Common


def find_data():
    rows = []
    sql = "select  supplierCode,supplierName,supplierCode as BRID,address1,address2, address3,address4,telephone," \
          "'1' as is_company from LECSupplierInfo where isActive = 1 union select freelanceCode as supplierCode, " \
          "concat(lastname, firstname) as supplierName, HKIDNumber as BRID, address1, address2, address3, address4, " \
          "telephone, '0' as is_company from LECFreelanceInfo where isActive = 1"
    Common.DB.execute(sql)
    res = Common.DB.fetchall()
    for r in res:
        supplierCode = r[0]
        supplierName = r[1]
        Shareholder_or_CO = ''
        BRID = r[2]
        Authorization_letter = ''
        address1 = r[3]
        address2 = r[4]
        address3 = r[5]
        address4 = r[6]
        telephone = r[7]
        is_company = 'Company' if r[8] == '1' else 'Individual'
        financial_controller = ''
        BPM_reference_number = ''

        sql = "SELECT concat(`User`.lastName, ' ', `User`.middleName) AS ename FROM `LECClaimHistory`" \
              " LEFT JOIN `User` ON `User`.staffID = LECClaimHistory.`user` WHERE `LECClaimCode` = " \
              "(SELECT processCode FROM `LECClaim` WHERE " \
              "(`expenseClaimType` = 'Supplier EC' OR `expenseClaimType` = 'Supplier Project EC') " \
              "AND `supplierCode` = '%s' ORDER BY id DESC LIMIT 1) AND (approvalGroups='DC' OR approvalGroups = 'L1' ) " \
              "ORDER BY field('approvalGroups', 'L1', 'DC'), datetime DESC LIMIT 1;" % (supplierCode)
        # logging.info(sql)
        Common.DB.execute(sql)
        res = Common.DB.fetchone()
        relevant_department_head = res[0] if res else ''

        row = [supplierCode, supplierName, is_company, Shareholder_or_CO, BRID, Authorization_letter, address1, address2, address3, address4,
               telephone, relevant_department_head, financial_controller, BPM_reference_number]
        logging.info(row)
        rows.append(row)
    return rows


def run():
    colums = [[
        'Approved vendor number',
        'Individual /Co Name',
        'Individual /company',
        'Shareholder/ director of the Co',
        'BR /ID copy',
        'Authorization letter',
        'address1',
        'address2',
        'address3',
        'address4',
        'Contact information (telephone)',
        'Relevant department head ',
        'Financial controller',
        'BPM reference number']]
    rows = find_data()
    Common.create_csv('supplier',colums, rows)
    pass


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
