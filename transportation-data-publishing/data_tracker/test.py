import sys

def cli_args():

    script_name = sys.argv[1]

    # parser = argparse.ArgumentParser()
    #
    # for arg in SCRIPTINFO[script_name]["arguments"]:
    #     parser.add_argument(arg, nargs="?",
    #                         type = str,
    #                         default = "")
    #
    # args = parser.parse.args
    #
    # args_dist = var(args)



    return script_name

if __name__ == "__main__":
    name = cli_args()
    print(name)
