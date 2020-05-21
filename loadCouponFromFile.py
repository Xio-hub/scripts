import sys
import logging
import include.Common as Common


def run():
    coupons = []
    with open("E:\\Users\\L\\PycharmProjects\\scripts\\data\\coupons.txt", "r") as f:  # 打开文件
        lines = f.readlines()  # 读取全部内容 ，并以列表方式返回
        for line in lines:
            coupons.append(line)

    sql = "INSERT INTO coupons(code) VALUES (%s)"

    try:
        Common.conn.begin()
        # 执行sql语句
        Common.DB.executemany(sql, coupons)
        # 提交到数据库执行
        Common.conn.commit()
    except Exception as e:
        Common.conn.rollback()


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
run
""" % sys.argv[0])
        sys.exit(1)

    Common.DB.close()
    logging.info('*' * 6 + ' script end ' + '*' * 6)