import sys

def word_count_dict(filename):
    word_count = {}
    input_file = open(filename, encoding='utf-8')
    for line in input_file:
        words = line.split()
        for word in words:
            word = word.lower()
            if not word in word_count:
                word_count[word] = 1
            else:
                word_count[word] = word_count[word] + 1
    input_file.close()
#    print(word_count)
    words = sorted(word_count.keys())
    words = words[:5] # Print only top 5
    for word in words:
        print(word, word_count[word])

def main():
    if len(sys.argv) != 3:
        print('usage: ./wordcount.py {--count | --topcount} file')
        sys.exit(1)

    filename=sys.argv[2]

    if (sys.argv[1]) == '--count':
        word_count_dict(filename)

if __name__ == '__main__':
    main()