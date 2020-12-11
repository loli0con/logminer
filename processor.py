import copy
import datetime
import os
import sys
import time

HOMEWORK_TITLE = "[作业]日志挖掘——处理数据"
MY_STUDENT_NUMBER = "201825010122"
INPUT_ENCODING = "GBK"
log = []
'''
学号: 201825010122
进出车次数: 9612
累计停放秒数: 625280951
'''


class Processor:
    def __init__(self, input_file_path):
        _path = os.path.abspath(input_file_path)
        if os.path.exists(_path):
            self.input_file = open(_path, encoding=INPUT_ENCODING)
        else:
            exit("can not find input file")
        self.records = list()
        self._init()

    def _init(self):
        global log
        log.append(time.time())
        for line in self.input_file:
            line = line.strip()

            dt, park_no, car_no, i_o = line.split(",")

            # 过滤条件
            if park_no != MY_STUDENT_NUMBER:
                continue

            _date, _time = dt.split(" ")
            # year, month, day, hour, minute, second
            _year, _month, _day = _date.split("-")
            _hour, _minute, _second = _time.split(":")

            dt = datetime.datetime(year=int(_year),
                                   month=int(_month),
                                   day=int(_day),
                                   hour=int(_hour),
                                   minute=int(_minute),
                                   second=int(_second)
                                   )
            record = {"datetime": dt, "car_no": car_no, "i_o": i_o}
            self.records.append(record)
        log.append(time.time())

    def calculate_1(self):
        return len(self.records) // 2

    def calculate_2(self):  # useless
        records = copy.deepcopy(self.records)

        def _compare(record):
            return str(record["car_no"]) + str(record["datetime"])

        # 排序，根据车牌号码和记录时间进行排序
        # 假数据里面，好像没有同一辆车重复进入同一个停车场的数据……？
        records.sort(key=_compare)

        total_stop_second = 0
        for i in range(1, len(records), 2):
            diff = records[i]["datetime"] - records[i - 1]["datetime"]
            total_stop_second += diff.total_seconds()
        return int(total_stop_second)

    def calculate_3(self):
        d = dict()
        total_stop_second = 0
        for record in copy.deepcopy(self.records):
            car_no = record["car_no"]

            if record["i_o"] == "out":
                diff = record["datetime"] - d.pop(car_no)
                total_stop_second += int(diff.total_seconds())
            else:
                d[car_no] = record["datetime"]
        return total_stop_second


def get_input_file_path():
    _file = os.path.basename(os.path.abspath(__file__))
    _input_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cars2.txt")

    last_argument = os.path.abspath(sys.argv[-1])
    if last_argument != os.path.abspath(__file__):
        _input_file_path = last_argument

    return _input_file_path


def print_run_time():
    def get_time_format(seconds):
        m, s = divmod(seconds, 60)
        h, m = divmod(m, 60)
        return "%02d:%02d:%02d." % (h, m, s)

    print("Start ReadFile:00:00:00.000")
    print("End ReadFile:" +
          get_time_format(int(log[1] - log[0])) +
          str(round(log[1] - log[0], 3)).split(".")[-1]
          )
    print("End Process:" +
          get_time_format(int(log[2] - log[0])) +
          str(round(log[2] - log[0], 3)).split(".")[-1]
          )


if __name__ == '__main__':
    print(HOMEWORK_TITLE)
    print("注意：自动读取日志文件,优先从命令行参数,其次从程序所在目录")

    print()
    input_file_path = get_input_file_path()
    _ = input("请输入日志文件的绝对路径(默认为" + str(input_file_path) + "):").strip()
    input_file_path = _ or input_file_path
    if os.path.exists(input_file_path):
        print("已确认文件" + input_file_path + "存在")
    else:
        exit("文件" + input_file_path + "不存在,请检查")

    print()
    _ = input("请输入您的学号(默认为" + MY_STUDENT_NUMBER + "):").strip()
    MY_STUDENT_NUMBER = _ or MY_STUDENT_NUMBER
    print("您的学号为" + MY_STUDENT_NUMBER)

    print()
    print("正在计算中,请稍后...")
    p = Processor(input_file_path)
    _1, _2, _3 = MY_STUDENT_NUMBER, p.calculate_1(), p.calculate_3()
    log.append(time.time())

    print("done")
    print()
    print("学号:", _1)
    print("进出车次数:", _2)
    print("累计停放秒数:", _3)

    print_run_time()
