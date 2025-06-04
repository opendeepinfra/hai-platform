import asyncclick as click
from .utils import HandleHfaiCommandArgs
import requests

from distutils.version import LooseVersion
from html.parser import HTMLParser
from importlib_metadata import version


latest_version: LooseVersion = '0.0.0'
try:
    current_version: LooseVersion = version('hfai').split('+')[0]
except:
    current_version: LooseVersion = 'unknown'

class IndexHtmlParser(HTMLParser):
    def handle_starttag(self, tag, attrs):
        global latest_version
        if tag == 'a':
            latest_version = max(
                latest_version, *[
                    LooseVersion(value.split('-')[1].split('%2B')[0])
                    for name, value in attrs if name == 'href'
                ])

def version_check():
    try:
        print(f'当前版本: {current_version}')
        resp = requests.get(
            'https://pypi.hfai.high-flyer.cn/simple/hfai/index.html', timeout=5)
        assert resp.status_code == 200, f'get pypi index failed {resp.status_code}'
        IndexHtmlParser().feed(resp.text)
        print(f'最新版本: {latest_version}')
        if current_version < latest_version:
            print(f'''Warning: 有可用的新版本: {latest_version}, 请通过以下命令更新:
            pip3 install hfai --extra-index-url https://pypi.hfai.high-flyer.cn/simple --trusted-host pypi.hfai.high-flyer.cn -U
    ''')
    except Exception as e:
        print(f'获取版本信息错误: {str(e)}')


@click.command(cls=HandleHfaiCommandArgs)
def version():
    """
    显示版本信息
    """
    version_check()
