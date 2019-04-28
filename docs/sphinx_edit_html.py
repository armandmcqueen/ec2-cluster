import os
from bs4 import BeautifulSoup




def fix_navpane(html_rel_path, module_name, entries):
    with open(os.path.abspath(html_rel_path), 'r') as f:
        html = f.read()

    out = []
    for line in html.split("\n"):

        # Find Nav pane entry, delete 'Module contents' entry and replace with direct links to entries
        if f'#module-{module_name}' in line and 'class="toctree-l4' in line:
            for entry in entries:
                new_navpane_entry = f'<li class="toctree-l4"><a class="reference internal" href="#{module_name}.{entry}">{entry}</a></li>'
                out.append(new_navpane_entry)

            continue

        out.append(line)

    final_out = "\n".join(out)
    with open(os.path.abspath(html_rel_path), 'w') as f:
        f.write(final_out)







if __name__ == '__main__':


    rel_path = "./_build/html/apidocs/ec2_cluster.infra.html"
    fix_navpane(rel_path, "ec2_cluster.infra", ["EC2Node", "EC2NodeCluster", "ConfigCluster"])

    rel_path = "./_build/html/apidocs/ec2_cluster.control.html"
    fix_navpane(rel_path, "ec2_cluster.control", ["ClusterShell"])

    rel_path = "./_build/html/apidocs/ec2_cluster.orch.html"
    fix_navpane(rel_path, "ec2_cluster.orch", ["add_to_known_hosts_cm",
                                               "check_ip_in_known_hosts_cmd",
                                               "set_up_passwordless_ssh_from_master_to_workers"])
