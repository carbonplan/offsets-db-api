# flake8: noqa

version = 'v0.0.1'

description = """

<img
 src="https://carbonplan-assets.s3.amazonaws.com/monogram/dark-small.png"
 alt="CarbonPlan Logo"
 align="left"
  width="80"
/>

<br/>
<br/>
<br/>
<br/>

Welcome to the CarbonPlan Offsets Database! This backend service provides an API for accessing the CarbonPlan Offsets Database.
The database contains information about carbon offsets projects, credits.
It also contains information about the offset credits that have been issued for each project.
The raw data are retrieved from the following registries:

- [art-trees](https://www.artredd.org/trees/)
- [climate action reserve](https://thereserve2.apx.com)
- [american carbon registry](https://acr2.apx.com/)
- [verra](https://registry.verra.org/)
- [gold-standard](https://www.goldstandard.org)
- [global carbon council](https://www.globalcarboncouncil.com/)

This API is deployed from the [carbonplan/offsets-db](https://github.com/carbonplan/offsets-db) repository.
If you have any questions or feedback, please open an issue in that repository.
"""


metadata = dict(
    title='CarbonPlan Offsets Database',
    description=description,
    contact=dict(name='CarbonPlan', url='https://github.com/carbonplan/offsets-db/issues'),
    license_info=dict(name='MIT License', url='https://spdx.org/licenses/MIT.html'),
    version=version,
)
