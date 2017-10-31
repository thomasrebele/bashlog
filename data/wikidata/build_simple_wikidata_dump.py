import gzip

import plac  # pip3 install plac


# Is going to return this data as a big file {subject}\t{property}\t{object}

def parse_triples(file_name):
    with gzip.open(file_name, 'rt') as file:
        for line in file:
            parts = [e.strip(' \r') for e in line.strip(' \t\r\n.').split(' ', 2)]
            if len(parts) == 3:
                yield parts
            else:
                print(parts)


predicates_map = {
    'P17': 'hasCountry',
    'P19': 'hasBirthPlace',
    'P20': 'hasDeathPlace',
    'P21': 'hasGender',
    'P22': 'hasFather',
    'P25': 'hasMother',
    'P26': 'hasSpouse',
    'P27': 'hasNationality',
    'P40': 'hasChild',
    'P131': 'isLocatedIn',
    'P150': 'containsLocation',
    'P3373': 'hasSibling',
    'P3448': 'hasStepParent'
}


def main(input_file):
    with open('all-triples.txt', 'wt') as fp:
        for (s, p, o) in parse_triples(input_file):
            fp.write('{}\t{}\t{}\n'.format(s, p, o))


if __name__ == '__main__':
    plac.call(main)
