import sys
import string
import random


def main():
    count = int(sys.argv[1])
    byte_size = int(sys.argv[2])
    beginning = int(sys.argv[3]) if len(sys.argv) > 3 else 0
    for key in range(beginning, beginning + count):
        rand = ''.join(random.choices(string.ascii_uppercase, k=byte_size))
        print(f"{key}:{rand}")

    for key in range(beginning, beginning + count):
        rand = ''.join(random.choices(string.ascii_uppercase, k=byte_size))
        print(f"{key}:{rand}")

if __name__== "__main__":
    main()
