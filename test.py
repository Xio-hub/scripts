import sys
import logging
import include.Common as Common
from libs.RedisQueue import RedisQueue


def test():
    # Common.red.set('test1', 'test string')
    # rq = RedisQueue('testQueue')
    # for i in range(5):
    #     rq.put(i)
    print(1)


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
test
""" % sys.argv[0])
        sys.exit(1)

    Common.DB.close()
    logging.info('*' * 6 + ' script end ' + '*' * 6)
