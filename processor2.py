import copy
import datetime
import os
import time
import sys
import multiprocessing
from collections import defaultdict

'''
学号: 201825010122
进出车次数: 9612
累计停放秒数: 625280951
3749288
'''
HOMEWORK_TITLE = "[作业]日志挖掘——并行改造"
MY_STUDENT_NUMBER = "201825010122"
INPUT_ENCODING = "GBK"


class Processor2(multiprocessing.Process):
    def __init__(self, q_log, input_file_path, start=0, size=None):
        super(Processor2, self).__init__()
        _path = os.path.abspath(input_file_path)
        if os.path.exists(_path):
            self.input_file = open(_path, "rb")
        else:
            pass
        self.q_log = q_log
        self.start_place = start
        self.size = size or os.path.getsize(input_file_path)
        self.records = list()

    def run(self):

        self.q_log.put({"processor": multiprocessing.current_process().name,
                        "type": "start read file",
                        "time": time.time()})
        self.read_file()
        self.q_log.put({"processor": multiprocessing.current_process().name,
                        "type": "finish read file",
                        "time": time.time()})

        result = self.calculate_1()
        self.q_log.put({"processor": multiprocessing.current_process().name,
                        "type": "finish c1",
                        "result": result,
                        "time": time.time()})

        result = self.calculate_2()
        self.q_log.put({"processor": multiprocessing.current_process().name,
                        "type": "finish c2",
                        "result": result,
                        "time": time.time()})

        self.q_log.put({"processor": multiprocessing.current_process().name,
                        "type": "finish all",
                        "time": time.time()})

    def read_file(self):  # 使用给定的文件对象，从start位置开始读取不小于等于size的字节
        self.input_file.seek(self.start_place, 0)
        # 修正start位置
        if self.start_place != 0:
            current_read = None
            separator = [b"\n", b"\r"]
            while True:
                pre_read = current_read
                current_read = self.input_file.read(1)
                if pre_read in separator and current_read not in separator:
                    self.input_file.seek(-1, 1)
                    break
        # 读取文件
        content_b = self.input_file.read(self.size) + self.input_file.readline()

        content_str = content_b.decode("GBK").strip().replace("\r", "")

        for one_line in content_str.split("\n"):
            self.process_line(one_line)

        # while self.size >= 0:
        #     one_line_byte = self.input_file.readline()
        #     if not one_line_byte:
        #         break
        #     self.size -= len(one_line_byte)
        #     one_line = one_line_byte.decode(INPUT_ENCODING).strip()
        #     self.process_line(one_line)

    def process_line(self, line):
        try:
            dt, park_no, car_no, i_o = line.split(",")
        except ValueError:
            print("error" + line)
            time.sleep(60)
        # 过滤条件
        if park_no != MY_STUDENT_NUMBER:
            return

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

    def calculate_1(self):
        result = 0
        for record in self.records:
            if record["i_o"] == "out":
                result += 1
        return result

    def calculate_2(self):
        d = defaultdict(list)
        stop_second = 0
        for record in copy.deepcopy(self.records):
            car_no = record["car_no"]
            if record["i_o"] == "in" or not d.get(car_no):
                d[car_no].append({"datetime": record["datetime"], "i_o": record["i_o"]})
            else:
                diff = record["datetime"] - d[car_no].pop()["datetime"]
                stop_second += int(diff.total_seconds())
        return stop_second, {key: value for key, value in d.items() if value}


class Dispatcher:
    def __init__(self, input_file_path="", works_number=os.cpu_count()):
        self.input_file_path = input_file_path or Dispatcher.get_input_file_path()
        if not os.path.exists(self.input_file_path):
            exit(self.input_file_path + " not exists")
        self.works_number = works_number
        self.q_log = multiprocessing.Queue()
        self.process_list = list()

    def init_processor(self):
        file_size = os.path.getsize(self.input_file_path)
        block_size = file_size // self.works_number
        start_point = 0
        for i in range(self.works_number - 1):
            p = Processor2(self.q_log, self.input_file_path, start_point, block_size)
            self.process_list.append(p)
            start_point += block_size
        p = Processor2(self.q_log, self.input_file_path, start_point, file_size - start_point)
        self.process_list.append(p)

    def start_processor(self):
        for p in self.process_list:
            p.start()

    def get_result(self):

        def insert_leave_records(old_dict: dict, new_dict: dict):
            for key, value in new_dict.items():
                if key in old_dict:
                    old_dict[key].extend(value)
                else:
                    old_dict[key] = value

        def cal_leave(d: dict):
            stop_second = 0
            for key, value in d.items():
                value.sort(key=lambda x: str(x.get("datetime")))
                for i in range(1, len(value), 2):
                    diff = value[i]["datetime"] - value[i - 1]["datetime"]
                    stop_second += diff.total_seconds()
            return stop_second

        finish_switch = self.works_number
        read_file_logs = list()
        # c1_logs = list()
        out_total = 0
        leave_records = dict()
        stop_time_total = 0
        while finish_switch:
            log: dict = self.q_log.get()
            log_type = log["type"]
            if log_type == "finish read file" or log_type == "start read file":
                read_file_logs.append(log)
            elif log_type == "finish c1":
                # c1_logs.append(log)
                out_total += log["result"]
            elif log_type == "finish c2":
                insert_leave_records(leave_records, log["result"][1])
                stop_time_total += log["result"][0]
                finish_switch -= 1

        stop_time_total += cal_leave(leave_records)
        finish_process_time = time.time()

        read_file_logs.sort(key=lambda x: x["time"])
        start_read_file_time = read_file_logs[0]["time"]
        finish_read_file_time = read_file_logs[-1]["time"]

        # print(read_file_logs)
        # print((start_read_file_time, finish_read_file_time, finish_process_time), (out_total, stop_time_total))

        return (start_read_file_time, finish_read_file_time, finish_process_time), (out_total, stop_time_total)

    def run(self):
        self.init_processor()
        self.start_processor()
        self.print_result(*self.get_result())

    def print_result(self, time_result: tuple, cal_result: dict):
        '''
        1、程序在读取日志文件前：

        Start ReadFile:00:00:00.000

        2、读取日志文件后：

        End ReadFile:xx:xx:xx.xxx

        3、对数据进行处理：

        End Process:xx:xx:xx.xxx
        '''

        def get_time_format(seconds):
            m, s = divmod(seconds, 60)
            h, m = divmod(m, 60)
            return "%02d:%02d:%02d." % (h, m, s)

        print("执行完成")
        print()

        print("执行日志：")
        print("Start ReadFile:00:00:00.000")
        print("End ReadFile:" +
              get_time_format(int(time_result[1] - time_result[0])) +
              str(round(time_result[1] - time_result[0], 3)).split(".")[-1]
              )
        print("End Process:" +
              get_time_format(int(time_result[2] - time_result[0])) +
              str(round(time_result[2] - time_result[0], 3)).split(".")[-1]
              )

        print()
        print("执行结果：")
        _1, _2, _3 = MY_STUDENT_NUMBER, cal_result[0], int(cal_result[1])
        print("学号:", _1)
        print("进出车次数:", _2)
        print("累计停放秒数:", _3)

    @staticmethod
    def get_input_file_path():
        _file = os.path.basename(os.path.abspath(__file__))
        _input_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cars2.txt")

        last_argument = os.path.abspath(sys.argv[-1])
        if last_argument != os.path.abspath(__file__):
            _input_file_path = last_argument

        return _input_file_path


def main():
    print(HOMEWORK_TITLE)
    print("注意：自动读取日志文件,优先从命令行参数,其次从程序所在目录")

    print()
    input_file_path = Dispatcher.get_input_file_path()
    _ = input("请输入日志文件的绝对路径(默认为" + str(input_file_path) + "):").strip()
    input_file_path = _ or input_file_path
    if os.path.exists(input_file_path):
        print("已确认文件" + input_file_path + "存在")
    else:
        exit("文件" + input_file_path + "不存在,请检查")

    print()
    global MY_STUDENT_NUMBER
    _ = input("请输入您的学号(默认为" + MY_STUDENT_NUMBER + "):").strip()
    MY_STUDENT_NUMBER = _ or MY_STUDENT_NUMBER
    print("您的学号为" + MY_STUDENT_NUMBER)

    print()
    _ = input("请输入生成进程数目(默认为" + str(os.cpu_count()) + "):").strip()
    works_number = int(_ or os.cpu_count())
    print("生成进程数为" + str(works_number))

    print()
    print("正在计算中,请稍后...")
    d = Dispatcher(input_file_path, works_number)
    d.run()


if __name__ == '__main__':
    main()
