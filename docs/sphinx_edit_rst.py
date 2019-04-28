import os
from bs4 import BeautifulSoup



def transform(rel_path, new_rel_path):
    with open(os.path.abspath(rel_path), 'r') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')

    print(soup)


def add_autosummary(rst_rel_path, entries):
    with open(os.path.abspath(rst_rel_path), 'r') as f:
        rst = f.read()

    summary = ["",
               ".. autosummary::",
               "   :toctree:",
               "   :nosignatures:",
               ""]

    summary.extend([f'   {entry}' for entry in entries])




    out = []
    for line in rst.split("\n"):
        out.append(line)
        if "=====" in line:
            out.extend(summary)
    print(rst)

    print("####")
    print(summary)
    print("----")

    final_out = "\n".join(out)

    print(">>>>>>>>>>>>>>>>>>>>>>>")
    print(final_out)
    print("///////////////////////")

    with open(os.path.abspath(rst_rel_path), 'w') as f:
        f.write(final_out)


def fix_navpane(html_rel_path, module_name, entries):
    with open(os.path.abspath(html_rel_path), 'r') as f:
        html = f.read()

    out = []
    for line in html.split("\n"):
        if f'#module-{module_name}' in line and 'class="toctree' in line:
            continue

        out.append(line)








if __name__ == '__main__':
    # rel_path = "./_build/html/apidocs/ec2_cluster.control.html"
    # transform(rel_path, "")



    rel_path = "./apidocs/ec2_cluster.control.rst"
    add_autosummary(rel_path, ["ec2_cluster.control.ClusterShell"])

    rel_path = "./apidocs/ec2_cluster.infra.rst"
    add_autosummary(rel_path, ["ec2_cluster.infra.EC2Node", "ec2_cluster.infra.EC2NodeCluster", "ec2_cluster.infra.ConfigCluster"])

    rel_path = "./apidocs/ec2_cluster.orch.rst"
    add_autosummary(rel_path, ["ec2_cluster.orch.add_to_known_hosts_cmd",
                               "ec2_cluster.orch.check_ip_in_known_hosts_cmd",
                               "ec2_cluster.orch.set_up_passwordless_ssh_from_master_to_workers"])

    rel_path = "./_build/html/apidocs/ec2_cluster.infra.html"
    fix_navpane(rel_path, "ec2_cluster.infra", ["EC2Node", "EC2NodeCluster", "ConfigCluster"])
