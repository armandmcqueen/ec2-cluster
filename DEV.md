# Dev Notes

### Improve README

https://github.com/18F/open-source-guide/blob/18f-pages/pages/making-readmes-readable.md

### Sphinx Docs
##### Google styleguide (with docstring info)

http://google.github.io/styleguide/pyguide.html#Comments

##### Add list of classes in docs

https://stackoverflow.com/questions/12150491/toc-list-with-all-classes-generated-by-automodule-in-sphinx-docs
https://github.com/tensorpack/tensorpack/blob/master/docs/_static/build_toc_group.js
http://www.sphinx-doc.org/en/1.6/ext/autosummary.html

##### Need to add the autosummary entries to the sidebar
docs/_build/html/apidocs/ec2_cluster.infra.html

Replace 
```
<li class="toctree-l4"><a class="reference internal" href="#submodules">Submodules</a></li>
<li class="toctree-l4"><a class="reference internal" href="#module-ec2_cluster.infra">Module contents</a></li>
```
with links to the entries in the autosummary table. Can find those entries by parsing the same file. Autosummary table can (probably) be parsed by finding `class="longtable docutils"`. Then parse HTML to get hrefs and the entry name (from title?)
```
<tbody valign="top">
<tr class="row-odd"><td><a class="reference internal" href="#ec2_cluster.infra.EC2Node" title="ec2_cluster.infra.EC2Node"><code class="xref py py-obj docutils literal notranslate"><span class="pre">ec2_cluster.infra.EC2Node</span></code></a></td>
<td>Class wrapping AWS SDK to manage EC2 instances.</td>
</tr>
<tr class="row-even"><td><a class="reference internal" href="#ec2_cluster.infra.EC2NodeCluster" title="ec2_cluster.infra.EC2NodeCluster"><code class="xref py py-obj docutils literal notranslate"><span class="pre">ec2_cluster.infra.EC2NodeCluster</span></code></a></td>
<td></td>
```

