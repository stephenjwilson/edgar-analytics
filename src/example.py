import sessionization
import sys


def example(log_file, inactivity_period, output_file):
    sessionizationObj = sessionization.Sessionization(log_file=log_file,
                                                      inactivity_period=inactivity_period,
                                                      output_file=output_file)
    sessionizationObj.run()


if __name__ == '__main__':
    log = sys.argv[1]
    inactivity = sys.argv[2]
    output = sys.argv[3]
    example(log, inactivity, output)
