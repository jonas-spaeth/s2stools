# ---
# jupyter:
#   jupytext:
#     cell_markers: '"""'
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.4
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
"""
# Plotting routines working with S2S data
"""

# %% [markdown]
"""
Imports:
"""

# %%
import s2stools
import xarray as xr
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# %% [markdown]
"""
Create data:
"""

# %%
np.random.seed(42)

n_member, n_timesteps = 10, 14
leadtime = pd.timedelta_range(start='0D', periods=n_timesteps, freq='1D')
da = xr.DataArray(
    np.cumsum(np.abs(np.random.normal(size=(n_member, n_timesteps))), axis=1),
    coords=dict(member=np.arange(n_member), leadtime=leadtime), dims=["member", "leadtime"]
)

da

# %% [markdown]
"""
Plot:
"""

# %%
da.plot(hue='member', figsize=(4,1), add_legend=False)
plt.title("timedelta on xaxis is in nanoseconds by default")
plt.show()

da.plot(hue='member', figsize=(4,1), add_legend=False)
s2stools.plot.xaxis_unit_days(multiple=2)
plt.title("timedelta on xaxis in days")
plt.show()

da.plot(hue='member', figsize=(4,1), add_legend=False)
s2stools.plot.xaxis_unit_days(multiple=2)
s2stools.plot.xlim_days(plt.gca(), 3,7)
plt.title("control x limits")
plt.show()
