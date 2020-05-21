import sys
import logging
import time
from multiprocessing import Pool
import include.Common as Common
from libs.RedisQueue import RedisQueue
import json

rq = RedisQueue('taskQueue')


def init():
    creat_result_file()
    sql = "select HKIDNumber,freelanceCode from LECFreelanceInfo where approved  = 0  and isActive  = 1"
    Common.DB.execute(sql)
    res = Common.DB.fetchall()
    for r in res:
        rq.put(json.dumps(r))


def creat_result_file():
    with open(Common.DATAPATH + 'update_freelance.sql', 'w', encoding='utf-8') as f:
        pass


def gen_sql():
    item = json.loads(str(rq.get_nowait(), encoding='utf-8'))
    hkid_number = item[0]
    sql = "select case when isnull(max(flcts.date)) then date_format(max(flcwd.date),'%Y-%c-%d %h:%i:%s') " \
          "else case when max(flcts.date) > max(flcwd.date) then date_format(max(flcwd.date),'%Y-%c-%d %h:%i:%s') " \
          "else date_format(max(flcwd.date),'%Y-%c-%d %h:%i:%s') end end " \
          "from LECFreelanceClaim flc left join LECFreelanceInfo fli on " \
          "fli.freelanceCode =flc.freelanceCode and flc.processStatus = 'Processed' left join " \
          "LECFreelanceClaimTimeSheet flcts on flcts.processCode = flc.processCode left join " \
          "LECFreelanceClaimWorkingDetails flcwd on flcwd.processCode = flc.processCode " \
          " where fli.HKIDNumber= '" + hkid_number + "' group by flc.freelanceCode"
    Common.DB.execute(sql)
    print(sql)
    res = Common.DB.fetchone()
    if res and (not res[0] is None) and compare_time(res[0], '2018-10-20 00:00:00'):
        sql = "update `LECFreelanceInfo` set approved = '1'  where   HKIDNumber= '%s';" % (hkid_number)
        print(sql)
        return sql


def write_sql_to_file(sql):
    with open(Common.DATAPATH + 'update_freelance.sql', 'a', encoding='utf-8') as f:
        f.writelines(sql)
        f.write('\n')


def compare_time(time1, time2):
    s_time = time.mktime(time.strptime(time1, '%Y-%m-%d %H:%M:%S'))
    e_time = time.mktime(time.strptime(time2, '%Y-%m-%d %H:%M:%S'))
    return True if int(s_time) - int(e_time) > 0 else False


def run():
    pool = Pool()
    while rq.qsize() > 0:
        pool.apply_async(gen_sql, callback=write_sql_to_file)
    pool.close()
    pool.join()


def checkDate():
    with open(Common.DATAPATH + 'freelance_check.txt', 'w', encoding='utf-8') as f:
        pass
    while rq.qsize() > 0:
        item = json.loads(str(rq.get_nowait(), encoding='utf-8'))
        hkid_number = item[0]
        freelance_code = item[1]
        sql = "select case when isnull(max(flcts.date)) then date_format(max(flcwd.date),'%Y-%c-%d %h:%i:%s') " \
              "else case when max(flcts.date) > max(flcwd.date) then date_format(max(flcwd.date),'%Y-%c-%d %h:%i:%s') " \
              "else date_format(max(flcwd.date),'%Y-%c-%d %h:%i:%s') end end " \
              "from LECFreelanceClaim flc left join LECFreelanceInfo fli on " \
              "fli.freelanceCode =flc.freelanceCode and flc.processStatus = 'Processed' left join " \
              "LECFreelanceClaimTimeSheet flcts on flcts.processCode = flc.processCode left join " \
              "LECFreelanceClaimWorkingDetails flcwd on flcwd.processCode = flc.processCode " \
              " where fli.HKIDNumber= '" + hkid_number + "' group by flc.freelanceCode"
        Common.DB.execute(sql)
        res = Common.DB.fetchone()
        if res:
            with open(Common.DATAPATH + 'freelance_check.txt', 'a', encoding='utf-8') as f:
                line = "HKIDNumber = %s;freelanceCode = %s;claim = %s;" % (hkid_number, freelance_code, res[0])
                print(sql, line)
                f.writelines(line)
                f.write('\n')
        else:
            with open(Common.DATAPATH + 'freelance_check.txt', 'a', encoding='utf-8') as f:
                line = "HKIDNumber = %s;freelanceCode = %s;claim = %s;" % (hkid_number, freelance_code, 'no claim')
                print(sql, line)
                f.writelines(line)
                f.write('\n')


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
init->run
Option
init
run
""" % sys.argv[0])
        sys.exit(1)

    Common.DB.close()
    logging.info('*' * 6 + ' script end ' + '*' * 6)
