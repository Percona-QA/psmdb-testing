#!/usr/bin/env python
import json
import os
from collections import deque

def resmoke2junit(skip_long_lines=1):
    """This function iterates over resmoke json result files in directory
     and converts the results to junit format suitable for Jenkins test results"""

    cwd = os.getcwd()
    error_log = deque("",200)

    with open('junit.xml', 'w') as junitfile:
        junitfile.write('<?xml version="1.0" ?>\n')
        junitfile.write('<testsuites>\n')

        for jsonfile in os.listdir(cwd):

            if jsonfile.startswith('resmoke') and jsonfile.endswith('.log'):
                jsonfileexists = '{}.json'.format(jsonfile[:-4])
                if jsonfileexists not in os.listdir(cwd):
                    junitfile.write('\t<testsuite name="{}" failures="{}" tests="{}">\n'.format(jsonfile[8:-6], 1, 1))
                    junitfile.write('\t\t<testcase name="{}" time="{}">'.format(jsonfile[8:-6], '0.0'))
                    junitfile.write('\n\t\t\t<failure><![CDATA[\n')
                    with open(jsonfile, 'r') as logfile:
                        error_log.clear()
                        for line in logfile:
                            line = ''.join(filter(lambda x: 32 <= ord(x) <= 126, line)) + '\n'
                            error_log.append(line)
                        for err in error_log:
                            junitfile.write(err)
                    junitfile.write('\t\t\t]]></failure>')
                    junitfile.write('\n\t\t</testcase>\n')
                    junitfile.write('\t</testsuite>\n')

            if jsonfile.startswith('resmoke') and jsonfile.endswith('.json'):
                with open(jsonfile) as data_file:
                    data = json.load(data_file)
                    junitfile.write('\t<testsuite name="{}" failures="{}" tests="{}">\n'.format(jsonfile[8:-7], data['failures'], len(data['results'])))

                    for result in data['results']:
                        junitfile.write('\t\t<testcase name="{}" time="{}">'.format(result['test_file'], result['elapsed']))
                        if result['status'] == 'fail':
                            junitfile.write('\n\t\t\t<failure><![CDATA[\n')
                            logfile = '{}.log'.format(jsonfile[:-5])
                            if '/' in result['test_file'][:-3]:
                                if jsonfile.startswith('resmoke_unittests'):
                                    test_name = result['test_file'].rsplit('/', 1)[1]
                                    prefix = '[cpp_unit_test:'
                                else:
                                    test_name = result['test_file'][:-3].rsplit('/', 1)[1]
                                    prefix = '[js_test:'
                            else:
                                test_name = result['test_file']
                                prefix = ''

                            has_error_text = 0
                            with open(logfile, 'r') as logfile:
                                error_log.clear()
                                for line in logfile:
                                    if '{}{}]'.format(prefix, test_name) in line:
                                        if len(line) >= 2048 and skip_long_lines == 1:
                                            error_log.append('### Skipped very long line ###\n')
                                        else:
                                            line = ''.join(filter(lambda x: 32 <= ord(x) <= 126, line)) + '\n'
                                            error_log.append(line)
                                        has_error_text = 1

                            if has_error_text == 0:
                                junitfile.write('Couldn\'t collect error text from the log file.]]></failure>')
                            else:
                                for err in error_log:
                                    junitfile.write(err)

                                junitfile.write('\t\t\t]]></failure>')

                        if result['status'] != 'pass':
                            junitfile.write('\n\t\t</testcase>\n')
                        else:
                            junitfile.write('</testcase>\n')
                    junitfile.write('\t</testsuite>\n')
                continue
            else:
                continue
        junitfile.write('</testsuites>')


if __name__ == "__main__":
    resmoke2junit()
