import os
import posixpath
import re


class Code:
    """
    用于提交任务时指定代码

    Args:
        directory (str): 代码所在的 workspace 目录
        entrypoint (str): 代码入口，可以是 .py 或者 .sh 文件
        parameters (str): 代码参数，默认为空

    Examples:

    .. code-block:: python

        from hfai.client import Code

        code = Code(directory='your/workspace/path', entrypoint='test.py')

        # 接下来将 code 用于生成 experiment，详见 create_experiment
    """

    def __init__(self, directory: str, entrypoint: str, parameters: str = ''):
        assert entrypoint.endswith('.sh') or entrypoint.endswith('.py'), f'代码入口必须是 shell 或者 py 文件: {entrypoint}'
        if os.name == 'nt':
            entrypoint = posixpath.join(*re.compile(r"[\\/]").split(entrypoint))
        if parameters is None:
            parameters = ''
        self.directory = directory
        self.entrypoint = entrypoint
        self.parameters = parameters

    def __repr__(self):
        return self.api_data()

    def api_data(self):
        """
        把资源输出成 rest api的样子
        :return: codefile, workspace
        """
        code_file = posixpath.join(self.directory, self.entrypoint)
        workspace = self.directory
        return f'{code_file} {self.parameters}'.strip(), str(workspace).strip()
