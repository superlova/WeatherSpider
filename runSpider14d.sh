#!/bin/bash
cd /home/gugezhang666/gitcode/WeatherSpider/tutorial/tutorial
/home/gugezhang666/anaconda3/bin/scrapy crawl NMCSpider14d >> /tmp/crawl.log & 
echo $(ps -ef | grep "scrapy crawl NMCSpider14d"  |grep -v 'grep' |awk -F ' ' '{print $2}')