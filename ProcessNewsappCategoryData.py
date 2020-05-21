import sys
import logging
import include.Common as Common

TABLES = ['cat_tag', 'favor_cat', 'user_favors']


def backup():
    """数据备份"""
    logging.info(Common.get_current_function_name())
    datestr = Common.NOW
    for tbl in TABLES:
        tbl_new = 'bak_' + datestr+'_' + tbl
        sql = "CREATE TABLE IF NOT EXISTS " +tbl_new+ " LIKE "+tbl+";"
        logging.info(sql)
        # Common.DB.execute(sql)

        sql = "INSERT "+tbl_new+" SELECT * FROM "+tbl+";"
        logging.info(sql)
        # try:
        #     Common.DB.execute(sql)
        #     Common.conn.commit()
        # except Exception as e:
        #     Common.conn.rollback()
        #     logging.error('MySQL error:' + str(e.args[0]) + str(e.args[1]))
        #     sys.exit(1)
        # logging.info('backup:' + tbl + ',' + tbl_new)
    return


def recover():
    """数据回滚"""
    logging.info(Common.get_current_function_name())
    datestr = Common.NOW
    for tbl in TABLES:
        tbl_bak = 'bak_' + datestr+'_' + tbl
        sql = "DROP TABLE " +tbl+";"
        logging.info(sql)
        sql = "RENAME TABLE "+tbl_bak+" TO "+tbl+";"
        logging.info(sql)
        # try:
        #     Common.DB.execute(sql)
        #     Common.conn.commit()
        # except Exception as e:
        #     Common.conn.rollback()
        #     logging.error('MySQL error:' + str(e.args[0]) + str(e.args[1]))
        #     sys.exit(1)
        # logging.info('recover:' + tbl + ' from ' + tbl_bak)
    return


def process_user_cat():
    sql = 'select user_id from user_favors where favor_id in (2,4,5,6,8) group by user_id'
    Common.DB.execute(sql)
    res = Common.DB.fetchall()
    for r in res:
        user_id = r[0]
        sql = 'delete from user_favors where user_id = %s and favor_id in (2,4,5,6,8);' % user_id
        logging.info(sql)
        Common.DB.execute(sql)

        values = (user_id, '8', '1', Common.NowDateTime, Common.NowDateTime)
        sql = 'INSERT INTO `user_favors`(`user_id`, `favor_id`, `is_active`, `created_at`, `updated_at`) ' \
              'VALUES (%s);' % ', '.join(map(lambda x: "'" + '%s' + "'", values))
        sql = sql % values
        logging.info(sql)
        Common.DB.execute(sql)
        Common.conn.commit()


def run():
    process_user_cat()


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
backup
recover
run
""" % sys.argv[0])
        sys.exit(1)

    Common.DB.close()
    logging.info('*' * 6 + ' script end ' + '*' * 6)