
import os
import json
import aiohttp
import enum
import requests
from asyncio import sleep
from urllib.parse import urlparse, parse_qs, urlunparse
from typing import Tuple
from .api_config import get_mars_bff_url, get_mars_token

def _parse_url(url: str) -> Tuple[(str, dict)]:
    bits = list(urlparse(url))
    params = {k: v[0] for (k, v) in parse_qs(bits[4]).items()}
    bits[4] = ''
    return (urlunparse(bits), params)

class RequestMethod(enum.Enum):
    GET = 0
    POST = 1

def check_client_version(client_version):
    if (not isinstance(client_version, str)):
        return
    if (client_version != 'NotFound'):
        client_version = (client_version.split('.') + (['0'] * 10))
        (major, minor, patch) = client_version[:3]
        try:
            from importlib_metadata import version
            current_version = version('hfai').split('+')[0]
            current_version = (current_version.split('.') + (['0'] * 10))
            (current_major, current_minor, current_patch) = current_version[:3]
            if ((int(major) - 1) > int(current_major)):
                print(f'[1;33m WARNING: [0m client版本过低，请及时更新；当前client版本: [1;33m{current_major}.{current_minor}.{current_patch}[0m，server版本: [1;33m{major}.{minor}.{patch}[0m', flush=True)
            elif (int(major) > int(current_major)):
                print(f'[1;35m WARNING: [0m client版本稍低，请及时更新；当前client版本: [1;35m{current_major}.{current_minor}.{current_patch}[0m，server版本: [1;35m{major}.{minor}.{patch}[0m', flush=True)
            if (int(major) > int(current_major)):
                print('可以通过 \x1b[1;36m pip3 install hfai --extra-index-url https://pypi.hfai.high-flyer.cn/simple --trusted-host pypi.hfai.high-flyer.cn --upgrade \x1b[0m 安装最新版本', flush=True)
        except:
            pass

async def async_requests(method: RequestMethod, url: str, assert_success: list=None, retries: int=1, allow_unsuccess: bool=False, **kwargs):
    '\n    向url发送一个异步请求\n\n    :param method: 0表示GET，1表示POST\n    :param url:\n    :param assert_success: list, 可以支持的success返回值\n    :param retries: 重试次数\n    :param allow_unsuccess: 是否允许不成功的请求\n    :return: 返回请求结果\n    '
    if (assert_success is None):
        assert_success = [1]
    timeout_seconds = kwargs.get('timeout', 60)
    timeout = aiohttp.ClientTimeout(total=timeout_seconds)
    for i in range(1, (retries + 1)):
        async with aiohttp.ClientSession(trust_env=True) as session:
            kwargs.pop('timeout', None)
            if (method == RequestMethod.POST):
                async_action = session.post(url=url, timeout=timeout, **kwargs)
            else:
                async_action = session.get(url=url, timeout=timeout, **kwargs)
            try:
                result = None
                async with (await async_action) as response:
                    headers = response.headers
                    client_version = headers.get('client-version', 'NotFound')
                    check_client_version(client_version)
                    result = (await response.text())
                    result = json.loads(result)
                    if (result.get('proxyError', None) == 'Timeout'):
                        raise Exception(f'服务端超时, {result}')
                    assert result.get('success', None), result
                    if (not allow_unsuccess):
                        assert (result['success'] in assert_success), result['msg']
                    elif (result['success'] not in assert_success):
                        print('\x1b[1;35m ERROR: \x1b[0m', f"请求失败，{result['msg']}")
                    return result
            except aiohttp.client_exceptions.ClientConnectorError as e:
                raise e
            except Exception as e:
                if (i == retries):
                    raise Exception(f'请求失败: [exception: {str(e)}] [result: {result}]')
                else:
                    print(f'第{i}次请求失败: {str(e)}, 等待2s后尝试重试...')
                    (await sleep(2))

def request_url(method: RequestMethod, url: str, assert_success: list=None, allow_unsuccess: bool=False, **kwargs):
    '\n    向url发送一个同步请求\n\n    :param method: 0表示GET，1表示POST\n    :param url:\n    :param assert_success: list, 可以支持的success返回值\n    :param allow_unsuccess: bool，是否允许不成功的请求\n    :return: 返回请求结果\n    '
    if (assert_success is None):
        assert_success = [1]
    if (method == RequestMethod.POST):
        res = requests.post(url, **kwargs)
    else:
        res = requests.get(url, **kwargs)
    headers = res.headers
    client_version = headers.get('client-version', 'NotFound')
    check_client_version(client_version)
    assert (res.status_code == 200), f'请求失败[{res.status_code}][{res.text}]'
    result = res.json()
    if (not allow_unsuccess):
        assert (result['success'] in assert_success), result['msg']
    elif (result['success'] not in assert_success):
        print('\x1b[1;35m ERROR: \x1b[0m', f"请求失败，{result['msg']}")
    return result
