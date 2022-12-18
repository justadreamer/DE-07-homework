import os

TEST_TOKEN = 'test'
os.environ.setdefault('API_AUTH_TOKEN',TEST_TOKEN)

def remove_dir(dir):
    if os.path.exists(dir):
        for f in os.listdir(dir):
            os.remove(os.path.join(dir, f))
        os.rmdir(dir)