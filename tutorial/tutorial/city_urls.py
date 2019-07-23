if __name__ == '__main__':
    city_url_dicts = {}

    url_real_prefix = "http://www.nmc.cn/f/rest/real/"

    with open('city_codes.txt', 'r', encoding='utf8') as f:
        for line in f:
            v = line.strip().split(' ')
            url = url_real_prefix + v[0]
            city_url_dicts[v[1]] = url
    print(city_url_dicts.values())
