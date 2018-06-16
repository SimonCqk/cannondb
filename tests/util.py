import os


def refine_test_file(file_name):
    if not os.path.exists('tmp'):
        os.mkdir('tmp')
    return os.path.join('tmp', file_name)
