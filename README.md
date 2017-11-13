# project

1) daem

This program realized connect from KafkaConsumer and Snort server. KafkaConsumer read JSON file from KafkaProducer ,parse this JSON and make file from snort rules in form (Example in rules/input):

1 line - Snort rules;

2 line - command(update, disable, enable);

3 line - path/to/file/name_file;

Working all daemon and logging in file all result. 

2) rules

This part parse rules/input and doing all necessery changes what write in this file. Doing this command: 

update - add new rules;

enable - add new or uncomment rules;

disable - add new from comment or comment old rules;

