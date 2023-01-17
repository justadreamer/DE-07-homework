import os


TEST_TOKEN = 'test'
os.environ.setdefault('API_AUTH_TOKEN', TEST_TOKEN)


def remove_dir(dirname):
    if os.path.exists(dirname):
        for f in os.listdir(dirname):
            os.remove(os.path.join(dirname, f))
        os.rmdir(dirname)
