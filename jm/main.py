
from data import my_dict

def main():
    ## iterate through dictionary of dictionarys to print pretty output
    print('Name | Age')
    for key, value in my_dict.items():
        temp_str = ''
        for k, v in value.items():
            temp_str += f'{v} | '
        print(temp_str[:-3])


if __name__ == '__main__':
    main()