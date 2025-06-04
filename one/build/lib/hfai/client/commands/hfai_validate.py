import asyncclick as click
from rich.console import Console

from hfai.client.api.experiment_api import validate_nodes, validate_experiment
from hfai.client.commands.utils import func_experiment_auto


class HandleHfaiArgs(click.Command):
    def format_usage(self, ctx, formatter):
        formatter.write_usage(f'''
        hfai validate <experiment> [<rank>...] [OPTIONS]               # 这个任务(<experiment>)的(<rank>)节点进行验证，默认为<rank>为all
        hfai validate nodes <node>... [OPTIONS]                        # 对输入的节点列表(<node>...)进行检查''')

    def format_help(self, ctx, formatter):
        super(HandleHfaiArgs, self).format_help(ctx, formatter)
        with formatter.section("Examples"):
            formatter.write_text('hfai validate bert.py 0 1 2         # 对bert.py这个任务rank为0,1,2三个节点进行validate')
            formatter.write_text('hfai validate nodes dl01 dl02 dl03  # 对 dl01 dl02 dl03 三个节点进行validate')

    def format_options(self, ctx, formatter) -> None:
        with formatter.section("Arguments"):
            formatter.write_dl(rows=[('experiment', '用于检索的任务，可以是任务名、是任务ID，也可以是提交的任务配置文件')])
            formatter.write_dl(rows=[('rank...', '验证任务的节点列表，如 0 1 2 则表示验证 0,1,2 三个节点 或者使用 all 表示所有节点，默认为: all')])
            formatter.write_dl(rows=[('node...', '和 validate nodes 一起使用，验证哪些节点，如 jd-a1010-dl jd-a1011-dl')])
        super(HandleHfaiArgs, self).format_options(ctx, formatter)

    def parse_args(self, ctx, args):
        for n, c in enumerate(args):
            if c.startswith('-t') or c.startswith('-f') or c in ['--exp_type', '--file']:
                args = args[0:1] + [tuple(args[1:n])] + args[n:]
                return super(HandleHfaiArgs, self).parse_args(ctx, args)
        args = args[0:1] + [tuple(args[1:])]
        return super(HandleHfaiArgs, self).parse_args(ctx, args)


@click.command(cls=HandleHfaiArgs)
@click.argument('experiment', metavar='experiment')
@click.argument('nodes', metavar='nodes')
@click.option('-t', '--exp_type', default='auto', help='配合 <experiment> 使用，默认 auto 会尝试解析')
@click.option('-f', '--file', default='/marsv2/scripts/validation/zhw_validate.py', help='测试代码路径，默认为系统组维护的测试代码；该任务失败会使得对应节点被挪出集群，请谨慎使用')
async def validate(experiment, nodes, exp_type, file):
    """
    检查节点正常情况
    """
    console = Console()
    if isinstance(nodes, str):
        nodes = eval(nodes)
    if experiment == 'nodes':
        if len(nodes) == 0:
            console.print('请至少指定一个节点进行 hfai validate nodes')
            exit(0)
        job_table = await validate_nodes(nodes, file=file)
    else:
        job_table = await func_experiment_auto(validate_experiment, experiment, exp_type, ranks=nodes, file=file)
    console.print(job_table['msg'])
