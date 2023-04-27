import time  # 导入time模块，用于计时
import os
from lxml import etree  # 导入etree模块，用于解析html
import concurrent.futures
from bs4 import BeautifulSoup
import requests  # 导入requests模块，用于发送http请求
import pandas as pd  # 导入pandas模块，用于读取excel数据
from fake_useragent import UserAgent
import time
import urllib3
import urllib.request
import urllib.parse
import datetime
import random
import urllib
import threading
import json
import ast
lock=threading.Lock()
urllib3.disable_warnings()

"""
加上整合小说以及
代理ip时间判断
"""

from concurrent.futures import ThreadPoolExecutor  # 导入ThreadPoolExecutor模块，用于多线程爬取数据

def get_log_name():
    # 获取当前时间
    now_time = datetime.datetime.now()
    # 格式化时间字符串
    str_time = now_time.strftime("%Y-%m-%d %H:%M:%S")
    now_time = time.time()
    return str_time, str(now_time)
def get_headers():
    """
    获取随机的UA
    :return:
    """
    ua = UserAgent()
    # 随机UA
    headers = {'user-agent': ua.random}
    return headers
def create_log(log_,flag,error_name,article,step):
    """
    日志记录
    :param log_:
    :param sec:
    :param error_name:
    :param article:
    :param step:
    :return:
    """
    if flag:
        sec_list=[5,9,13,7,11,6,12,8,4,3]
        sec=random.choice(sec_list)
        time.sleep(sec)
    now_date, time_name = get_log_name()
    with open(log_,'a+',encoding='utf-8')as f:
        f.write('时间：'+now_date+'文章《'+article+'》的<'+str(step)+'>步骤,'+error_name+'\n')
    # print('文章《'+article+'》的<'+step+'>步骤,'+error_name)

def FileName(STR):
    """
    保存的文本txt文件名称非法的情况下的处理措施
    :param STR:
    :return:
    """
    for i,j in ("/／","\\＼","?？","|︱","\"＂","*＊","<＜",">＞"):
        STR=STR.replace(i,j)
    return STR
def is_Filename(STR):
    """
    保存的文本txt文件名称非法的情况下的处理措施
    :param STR:
    :return:
    """
    for i,j in ("/／","\\＼","?？","|︱","\"＂","*＊","<＜",">＞"):
        if i in STR:
            STR=STR.replace(i,j)
    return STR
def get_book_name_and_author(excel_name):
    """
       读取excel文件中的书名和作者，并返回列表形式的数据
       :param excel_name: excel文件名
       :return: 书名和作者组成的列表
       """
    df = pd.read_excel(excel_name)  # 读取excel文件
    df = df.loc[:, ["书名", "作者名"]]  # 获取"书名"和"作者名"列的数据
    df = df.iloc[1700:1800]  # 只获取前1500个数据
    result_list = []
    for data in df.values:
        result_list.append(data.tolist())
    return result_list
def get_proxies_(log_,num,flag):

    num=str(num)
    #这个函数在程序刚开始就先申请100个ip
    # print('获取之前，先写个日志')
    create_log(log_,False, '获取中...', flag, '获取代理IP')
    # print('获取中')
    url='http://http.tiqu.letecs.com/getip3?num='+num+'&type=2&pro=&city=0&yys=0&port=11&pack=311045&ts=1&ys=0&cs=0&lb=1&sb=0&pb=4&mr=1&regions='
    response = requests.get(url, headers=get_headers())
    response.encoding = 'utf-8'
    ip_dict_str=response.text
    ip_dict_str=ip_dict_str.replace('true','True')
    # data_list=ip_dict.get('data')
    create_log(log_,False, '获取成功！'+ip_dict_str, flag, '获取代理IP'+num+'个')
    # print('获取成功')
    return ip_dict_str

def get_time_late(target_time):

    format_pattern = '%Y-%m-%d %H:%M:%S'
    # print(1)
    cur_time = datetime.datetime.now()+ datetime.timedelta(minutes=3)
    # print(cur_time)

    # 将 'cur_time' 类型时间通过格式化模式转换为 'str' 时间
    cur_time = cur_time.strftime(format_pattern)
    if  target_time < cur_time:
        return False
    else:
        return True

def is_plus_prox(log_,proxy_path,proce_num):
    with open(proxy_path,'r',encoding='utf-8')as read:
        dict_str=read.read()
    ip_dict=eval(dict_str)
    # print(ip_dict)
    data_list=ip_dict.get("data")
    #判断失活的ip是否低于40
    # print(data_list)

    ip_dict_list=[]
    for data in data_list:
        if get_time_late(data.get('expire_time')):
            # print(data)
            ip_dict_list.append(data)
    # print(ip_dict_list)
    # print('之前长度',len(ip_dict_list))

    if len(ip_dict_list) < int(proce_num*0.8):
        ip_dict_str=get_proxies_(log_,int(proce_num*8),'新增IP')
        ip_dict_str=eval(ip_dict_str)
        list_=ip_dict_str.get("data")
        ip_dict_list.extend(list_)

        # print(ip_dict_list)
        # print('之后长度',len(ip_dict_list))

        ip_dict.update({'data':ip_dict_list})

    # print(ip_dict_str)

    with open(proxy_path,'w',encoding='utf-8')as w:
        w.write(str(ip_dict))
    # print(ip_dict)
    return ip_dict_list

def is_article(book_name,html_parser,book_author):
    article_text = html_parser.xpath('//*[@id="info"]/h1/text()')
    # print(article_text[0])
    if len(article_text) !=0:
        text=article_text[0]
        author = html_parser.xpath('//*[@id="info"]/p/text()')[0]
        author = author.replace("\xa0\xa0\xa0\xa0", '')
        author = author.replace("作者：", '')
        # print(repr(author))
    else:
        return 0
    if book_name == text and book_author in author:
        return 1
    else:
        return 2

def recursion_dir_all_file(path):
    file_list = []
    for dir_path, dirs, files in os.walk(path):
        for file in files:
            file_path = os.path.join(dir_path, file)
            if file_path.endswith('.txt'):
                #只留下.docx为结尾的文件
                file_list.append(file_path)
    return file_list
def remo_novel(path):
    file_list=recursion_dir_all_file(path)
    for file in file_list:
        os.remove(file)
    os.rmdir(path)

def get_novel_pool(log_,book_name,path_,net_url,context_url_list,proxy_path,proce_num):
    print(1)
    def send_novel_request(log_,url,step):
        """
        作用是发送请求，并且错误进行响应
        :param log_:
        :param url:
        :param book_name:
        :param step:
        :return:
        """
        max_retry_num = 10  # 最大重试次数
        while max_retry_num > 0:
            try:
                # print(max_retry_num)
                start_time = time.time()
                # print('正在发送')
                lock.acquire()  # 获取线程锁
                try:
                    ip_list=is_plus_prox(log_,proxy_path,proce_num)
                finally:
                    lock.release()  # 释放线程锁
                # with lock:
                #
                ip_dict=random.choice(ip_list)
                # print(ip_dict)
                ip=ip_dict.get('ip')
                port=ip_dict.get('port')
                ip_port=ip+':'+str(port)
                # print(ip_port)
                proxy={'https':ip_port}
                # print(proxy)
                session = requests.session()
                # print(url)
                response = session.get(url, headers=get_headers(),proxies=proxy,verify=False)

                code = response.status_code
                # print(code)
            except ConnectionRefusedError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True, 'ConnectionRefusedError'+'代理：'+str(proxy), book_name, step)
                continue
            except TimeoutError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True,'TimeoutError'+'代理：'+str(proxy), book_name, step)
                continue
            except ConnectionAbortedError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True,'ConnectionAbortedError'+'代理：'+str(proxy), book_name, step)
                continue
            except urllib3.exceptions.ProtocolError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True,'urllib3.exceptions.ProtocolError'+'代理：'+str(proxy), book_name, step)
                continue
            except urllib3.exceptions.NewConnectionError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True,'urllib3.exceptions.NewConnectionError'+'代理：'+str(proxy), book_name, step)
                continue
            except urllib3.exceptions.MaxRetryError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True,'urllib3.exceptions.MaxRetryError'+'代理：'+str(proxy), book_name, step)
                continue
            except requests.exceptions.ConnectionError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True,'requests.exceptions.ConnectionError'+'代理：'+str(proxy), book_name, step)
                continue
            except urllib.error.URLError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True,'urllib.error.URLError'+'代理：'+str(proxy), book_name, step)
                continue
            else:
                end_time = time.time()

                if code != 200:
                    max_retry_num = max_retry_num - 1
                    create_log(log_,True, '请求链接返回码错误'+'代理：'+str(proxy), book_name, step)
                    continue
                elif end_time - start_time > 7000:
                    max_retry_num = max_retry_num - 1
                    create_log(log_,True, '超时重传'+'代理：'+str(proxy), book_name, step)
                    continue
                else:
                    # sec_list = [1, 2, 3, 4, 5]
                    # sec = random.choice(sec_list)
                    # time.sleep(sec)
                    response.encoding = 'GBK'
                    html = response.text
                    html_parser = etree.HTML(html)
                    return html,html_parser,response,proxy
        return False,False,False

    url = net_url + context_url_list[1]
    print(url)
    html, html_parser,response,proxy_ = send_novel_request(log_,url,context_url_list[0])
    if html == False:
        return True
    else:
        soup = BeautifulSoup(html, "lxml")
        text_ = soup.find('div', id='content').text
        text_ = text_.replace("\xa0\xa0\xa0\xa0", '\n\n    ')

        with open(path_ + context_url_list[0] + '.txt', 'a+', encoding='utf-8') as f:
            f.write(text_)
        create_log(log_,False,'下载成功！'+str(proxy_), book_name, '下载章节' + context_url_list[0])
        return False

def begin_spide(log_,book_name,author,succ_,not_fund,error_title,proxy_path,path,proce_num):

    def get_url_of_article(html_parser):
        """
        解析出搜索结果的小说以及作者和链接列表
        :param html_parser:
        :return:
        """
        li=html_parser.xpath('//*[@id="content"]/table/tr')
        li_list=li[1:len(li)]#去掉标题行
        data_list=[]
        for li in li_list:
            span=li.xpath('./td')
            # print(span)
            temp_list=[]
            temp_list.append(span[0].xpath('./a/text()')[0])#小说名称
            temp_list.append(span[2].xpath('./text()')[0])#作者名称
            temp_list.append(span[0].xpath('./a/@href')[0])#小说链接
            data_list.append(temp_list)
        # print(data_list)
        return data_list

    def send_request(log_,url,book_name,step,proxy_path):
        """
        作用是发送请求，并且错误进行响应
        :param log_:
        :param url:
        :param book_name:
        :param step:
        :return:
        """
        max_retry_num = 20  # 最大重试次数
        while max_retry_num > 0:
            try:
                # print(max_retry_num)
                start_time = time.time()
                # print('正在发送')
                lock.acquire()  # 获取线程锁
                try:
                    ip_list=is_plus_prox(log_,proxy_path,proce_num)
                finally:
                    lock.release()  # 释放线程锁
                # with lock:
                #
                ip_dict=random.choice(ip_list)
                # print(ip_dict)
                ip=ip_dict.get('ip')
                port=ip_dict.get('port')
                ip_port=ip+':'+str(port)
                # print(ip_port)
                proxy={'https':ip_port}
                # print(proxy)
                session = requests.session()
                # print(url)
                response = session.get(url, headers=get_headers(),proxies=proxy,verify=False)

                code = response.status_code
                # print(code)
            except ConnectionRefusedError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True, 'ConnectionRefusedError'+'代理：'+str(proxy), book_name, step)
                continue
            except TimeoutError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True,'TimeoutError'+'代理：'+str(proxy), book_name, step)
                continue
            except ConnectionAbortedError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True,'ConnectionAbortedError'+'代理：'+str(proxy), book_name, step)
                continue
            except urllib3.exceptions.ProtocolError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True,'urllib3.exceptions.ProtocolError'+'代理：'+str(proxy), book_name, step)
                continue
            except urllib3.exceptions.NewConnectionError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True,'urllib3.exceptions.NewConnectionError'+'代理：'+str(proxy), book_name, step)
                continue
            except urllib3.exceptions.MaxRetryError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True,'urllib3.exceptions.MaxRetryError'+'代理：'+str(proxy), book_name, step)
                continue
            except requests.exceptions.ConnectionError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True,'requests.exceptions.ConnectionError'+'代理：'+str(proxy), book_name, step)
                continue
            except urllib.error.URLError:
                max_retry_num = max_retry_num - 1
                create_log(log_,True,'urllib.error.URLError'+'代理：'+str(proxy), book_name, step)
                continue
            else:
                end_time = time.time()

                if code != 200:
                    max_retry_num = max_retry_num - 1
                    create_log(log_,True, '请求链接返回码错误'+'代理：'+str(proxy), book_name, step)
                    continue
                elif end_time - start_time > 7000:
                    max_retry_num = max_retry_num - 1
                    create_log(log_,True, '超时重传'+'代理：'+str(proxy), book_name, step)
                    continue
                else:
                    # sec_list = [1, 2, 3, 4, 5]
                    # sec = random.choice(sec_list)
                    # time.sleep(sec)
                    response.encoding = 'GBK'
                    html = response.text
                    html_parser = etree.HTML(html)
                    return html,html_parser,response,proxy
        return False,False,False

    def get_category_url(log_,book_name, author,proxy_path):
        """
                根据书名和作者名，获取小说分类页面的url
                :param book_name: 书名
                :param author: 作者名
                :return: 小说分类页面的url
                """
        new_book_name = urllib.parse.quote(book_name.encode('gb2312'))
        create_log(log_,False, '正在搜索...', book_name, '根据小说名称和作者搜索')
        url = "https://www.23dd.cc/modules/article/search.php?searchkey="+str(new_book_name)
        html1,html_parser,response,proxy_=send_request(log_,url,book_name, '获取小说链接',proxy_path)

        #编码转换

        flag=is_article(book_name,html_parser,author)
        if flag == 1:
            # print('直接搜索到小说')
            # print(url)
            response = requests.get(url, allow_redirects=True)
            final_url = response.url
            # print("重定向之后的链接：", final_url)
            return final_url
        elif flag == 2:
            # print("搜到的小说不对应")
            return False
        else:
            #flag=0的时候
            # print("解析链接")
            data_list=get_url_of_article(html_parser)
            for data in data_list:
                #寻找标题和作者对应的小说
                if data[0] == book_name and data[1] == author:
                    return data[2]
            return False
    def get_all_context_url(log_,book_name,article_url,proxy_path):
        """
        获取当前小说的所有章节的链接，返回列表
        :param article_url:
        :return:
        """
        create_log(log_,False, '正在解析...', book_name, '解析当前小说的目录章节链接')
        html, html_parser,response,proxy_ = send_request(log_,article_url, book_name, '获取章节目录链接',proxy_path)

        dd_list=html_parser.xpath('//*[@id="list"]/dl/dd')

        # print(dd_list)
        # print(article_url)

        context_list=[]
        for dd in dd_list:
            temp_list=[]
            context_name=dd.xpath('./a/text()')[0]
            # print(context_name)
            context_url=dd.xpath('./a/@href')[0]
            # print(context_url)
            temp_list.append(context_name)
            temp_list.append(context_url)
            context_list.append(temp_list)
        # print(context_list)
        create_log(log_,False, '解析成功，开始下载', book_name, '解析当前小说的目录章节链接')
        return context_list
    def get_novel(log_,book_name,context_url_list,net_url,error_title,proxy_path,path_,novel_path):
        """
        对小说的章节的链接的对应内容进行下载
        :param log_:
        :param book_name:
        :param author:
        :param context_url_list:
        :param net_url:
        :return:
        """
        #随机打乱列表数据
        for i in range(len(context_url_list)):
            context_url_list[i][0]=is_Filename(context_url_list[i][0])
        #记录小说顺序的变量
        sec_list=[]
        for context in context_url_list:
            sec_list.append(context)
        random.shuffle(context_url_list)

        context_num=False
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # 提交内层任务并获取 Future 对象
            print('开始提交')
            inner_futures = [executor.submit(get_novel_pool,log_,book_name,path_,net_url,context_url_list[i],proxy_path,proce_num) for i in range(len(context_url_list))]
            # 遍历内层 Future 对象列表，获取任务的返回值
            print('提交线程')
            for inner_future in concurrent.futures.as_completed(inner_futures):
                context_num = inner_future.result()
        # pool = ThreadPoolExecutor(max_workers=2)
        # context_num=False
        # for i in range(len(context_url_list)):
        #     print(context_url_list[i])
        #     future=pool.submit(get_novel_pool,log_,book_name,path_,net_url,context_url_list[i],proxy_path,proce_num)
        #     context_num = future.result()

        #如果当前小说有章节下载失败，那么删除以及下载的文件，并且记录
        if context_num:
            print('当前章节下载失败!   停止下载当前小说...'+book_name+'\n')
            with open(error_title,'a+',encoding='utf-8') as f:
                f.write('小说：'+book_name+'&&章节：'+context_url_list[i][0]+'\n')
            if os.path.exists(path_):
                remo_novel(path_)
            create_log(log_,False,'下载失败！，已删除当前小说全部章节'+book_name, book_name, '下载小说')
        else:
            create_log(log_,False,'下载成功！！！正在切换小说...\n', book_name, '下载全部章节' + context_url_list[i][0])
            print(book_name+'  '+'下载成功!！！！正在切换小说...\n')
            #整合下载成功的小说
            with open(novel_path,'a+',encoding='utf-8') as f:
                for sec in sec_list:
                    # print('写入'+sec[0]+'章')
                    with open(path_+sec[0]+'.txt','r',encoding='utf-8') as r:
                        text=r.read()
                    f.write(text+'\n')
            with open(succ_,'a+',encoding='utf-8') as f:
                f.write(book_name+'\n')


    net_url='https://www.23dd.cc/'
    book_url = get_category_url(log_, book_name, author,proxy_path)
    if book_url == False:
        # print("未发现")
        create_log(log_,False,'未发现该小说', book_name, '搜索结果')
        with open(not_fund, 'a+', encoding='utf-8') as file:
            file.write(book_name + '——' + author + '\n')
        return False
    else:
        # print("发现")
        create_log(log_,False,'搜索成功，开始解析章节链接', book_name, '搜索结果')
        # print(net_url+book_url)
        #获取文章目录链接
        path_ = './小说中转站/'+book_name+'/'
        book_name=is_Filename(book_name)
        print(book_name)
        if os.path.exists(path_):
            remo_novel(path_)

        os.makedirs(path_)
        novel_path=path+'/'+book_name+'.txt'
        with open(novel_path,'w',encoding='utf-8') as f:
            f.write('')
        if 'https' in book_url:
            context_url_list = get_all_context_url(log_, book_name, book_url,proxy_path)
            get_novel(log_, book_name, context_url_list, book_url,error_title,proxy_path,path_,novel_path)
        else:
            context_url_list = get_all_context_url(log_, book_name, net_url + book_url,proxy_path)
            get_novel(log_, book_name, context_url_list, net_url + book_url,error_title,proxy_path,path_,novel_path)


def make_dir(path):
    if not os.path.exists('./'+path+'/'):
        os.makedirs('./'+path+'/')
def main():

    now_date, time_name = get_log_name()
    make_dir('爬虫日志')
    make_dir('成功书单')
    make_dir('未发现小说记录')
    make_dir('错误章节')
    make_dir('代理IP列表')

    path = './书单下载/'
    if not os.path.exists(path):
        os.makedirs(path)

    log_ = './爬虫日志/' + time_name+'.log'
    succ_ = './成功书单/成功书单' + time_name+'.txt'
    not_fund='./未发现小说记录/未发现小说名单'+time_name+'.txt'
    error_title='./错误章节/错误小说章节记录'+time_name+'.txt'
    proxy_path='./代理IP列表/代理ip'+time_name+'.txt'

    create_log(log_,False,now_date, '记录开始时间', '生成当前时间')

    proce_num=1

    pool = ThreadPoolExecutor(max_workers=proce_num)
    book_info_list = get_book_name_and_author("未完成1.xlsx")

    # book_info_list=[['快穿之时景的拯救之旅','真真真小远']]
    print(book_info_list)
    ip_dict_str=get_proxies_(log_,int(proce_num*5),'获取初始ip')
    # print(ip_dict_str)
    with open(proxy_path, 'w', encoding='utf-8') as file:
        file.write(ip_dict_str)

    for book_info in book_info_list:
        book_name, author = book_info
        pool.submit(begin_spide,log_, book_name, author,succ_,not_fund,error_title,proxy_path,path,proce_num)
if __name__ == "__main__":
    main()