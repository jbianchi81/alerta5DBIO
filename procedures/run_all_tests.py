import os
# run all tests
def runAllTests(dir_path,break_on_error):
    test_files = os.listdir(dir_path)
    test_files = [f for f in test_files if os.path.isfile(dir_path+'/'+f) and f.endswith(".yml")]
    returned_value_sum = 0
    for test_file in test_files:
        returned_value = os.system("a5cli run %s/%s -t" % (dir_path,test_file))
        if returned_value > 0 and break_on_error:
            print("Error running test %s/%s" % (dir_path,test_file))
            return returned_value
        returned_value_sum = returned_value_sum + returned_value
    return returned_value_sum

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('dir_path',type=str,help='path of the directory where test procedure files are stored')
    parser.add_argument('--break_on_error', action='store_true', help='exit on first error found')
    args = parser.parse_args()
    returned_value = runAllTests(args.dir_path,args.break_on_error)
    exit(returned_value) 