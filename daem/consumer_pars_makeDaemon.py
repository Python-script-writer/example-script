import logging, multiprocessing, sys, time

from functools import reduce
from daemon3 import Daemon
from kafka import KafkaConsumer

# input hostname:port(192.168.20.123:9092) and topic (my-topic1)
host_port = input('hostname:port: ')
topic = input('topic: ')

logging.basicConfig(level=logging.CRITICAL, filename="test.log", format="%(message)s")


class Consumer(multiprocessing.Process):
    def __init__(self):
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()

    def stop(self):
        self.stop_event.set()

    def run(self):
        consumer = KafkaConsumer(bootstrap_servers=host_port, auto_offset_reset='earliest', consumer_timeout_ms=1000, )
        consumer.subscribe([topic])

        for message in consumer:
            if self.stop_event.is_set():
                break

            string = str(message)

            def string1():
                l = string.split('=')[7]
                m = l.split("'")[1]
                r = m.split("[")
                addit_op = r[1].split("]")[0]
                addit_op_1 = addit_op.split(",")

                json_file_1 = {}

                json_file_1["msg"] = m.split("msg")[1].split(":")[1].split(",")[0].split("}")[0].split('"')[1]
                json_file_1["protocol"] = \
                    m.split("protocol")[1].split(":")[1].split(",")[0].split("}")[0].split('"')[1]
                json_file_1["sourceIp"] = \
                    m.split("sourceIp")[1].split(":")[1].split(",")[0].split("}")[0].split('"')[1]
                json_file_1["sourcePort"] = m.split("sourcePort")[1].split(":")[1].split(",")[0].split("}")[0]
                json_file_1["destinationIp"] = \
                    m.split("destinationIp")[1].split(":")[1].split(",")[0].split("}")[0].split('"')[1]
                json_file_1["destinationPort"] = \
                    m.split("destinationPort")[1].split(":")[1].split(",")[0].split("}")[0]

                dict1 = {}
                for n in range(len(addit_op_1)):
                    dict1['{}'.format(addit_op_1[n].split('"')[1]).split("}")[0]] = '{}'.format(
                        addit_op_1[n].split('"')[2].split()[1].split("}")[0])

                json_file_1["additionalOptions"] = [dict1]
                return json_file_1

            def key0():
                list0 = []
                for key in string1()['additionalOptions'][0].keys():
                    list0.append(key)
                return list0

            l = []
            for n in range(len(string1()['additionalOptions'][0])):
                l.append(n)

            map_list = list(map(lambda m: r' {}:'.format(str(key0()[m])) + str(
                string1()['additionalOptions'][0][str(key0()[m])]) + r';', l))
            reduce_all = reduce(lambda x, y: str(x) + str(y), map_list)

            def parser():
                return ('alert ' + string1()["protocol"] + " " + string1()["sourceIp"] + " " + str(
                    string1()["sourcePort"]) + " -> " + string1()["destinationIp"] + " " + str(
                    string1()["destinationPort"]) + r' (msg:"' + string1()["msg"] + r'";' + reduce_all + ')\n' + str(
                    string1()["command"]) + "\n" + str(string1()["file"]))

            logging.critical(parser())

        consumer.close()


def main():
    tasks = [Consumer()]

    for t in tasks:
        t.start()

    time.sleep(1)

    for task in tasks:
        task.stop()

    for task in tasks:
        task.join()


class MyDaemon(Daemon):
    def run(self):
        main()


if __name__ == "__main__":
    daemon = MyDaemon('/tmp/daem.pid')
    if len(sys.argv) == 2:
        if 'start' == sys.argv[1]:
            daemon.start()
        elif 'stop' == sys.argv[1]:
            daemon.stop()
        elif 'restart' == sys.argv[1]:
            daemon.restart()
        else:
            print("Unknown command")
            sys.exit(2)
        sys.exit(0)
    else:
        print("usage: %s start|stop|restart" % sys.argv[0])
        sys.exit(2)
