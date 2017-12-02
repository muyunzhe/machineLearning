# include parse_yaml function
. parse_yaml.sh

# read yaml file
eval $(parse_yaml sample.yml "config_")

# access yaml content
echo ${config_output_file}