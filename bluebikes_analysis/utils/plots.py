import random as rnd

import contextily as ctx
import geopandas as gpd
import matplotlib.pyplot as plt
from pyproj import Transformer
from shapely.geometry import Point

COLORS = [
    "#f1b6da",
    "#c51b7d",
    "#fdae61",
    "#d73027",
    "#a6d96a",
    "#1a9850",
    "#abd9e9",
    "#2166ac",
    "#b2abd2",
    "#542788",
    "#bababa",
    "#4d4d4d",
]


def plot_daily_longterm(
    df_daily,
    columns_to_plot,
    title="",
    xlabel="",
    ylabel="",
    color_list=None,
    linestyle_list=None,
    legend_labels=None,
    figsize=(12, 6),
    ylim_max=None,
):
    """
    Plot daily/long-term time series profile.

    Parameters:
    -----------
    df_daily : pd.DataFrame
        DataFrame indexed by date/timestamp with columns to plot
    columns_to_plot : list
        List of column names to plot
    title : str, optional
        Plot title
    xlabel : str, optional
        X-axis label
    ylabel : str, optional
        Y-axis label
    color_list : list, optional
        Colors for each column (default: uses first N from colors)
    linestyle_list : list, optional
        Line styles for each column (default: '-' for all)
    legend_labels : list, optional
        Legend labels for each column (default: column names)
    figsize : tuple, optional
        Figure size (width, height)
    ylim_max : float, optional
        Maximum y-axis limit (default: None, auto-scales)
    """
    # Set defaults
    if color_list is None:
        color_list = COLORS[: len(columns_to_plot)]
    if linestyle_list is None:
        linestyle_list = ["-"] * len(columns_to_plot)
    if legend_labels is None:
        legend_labels = columns_to_plot

    plt.figure(figsize=figsize)

    # Plot each column
    for col, color, linestyle, label in zip(
        columns_to_plot, color_list, linestyle_list, legend_labels
    ):
        plt.plot(
            df_daily.index,
            df_daily[col],
            color=color,
            linewidth=1.5,
            linestyle=linestyle,
            label=label,
            alpha=0.8,
            marker="o",
            markersize=2,
        )

    if title:
        plt.title(title, fontsize=15)
    if xlabel:
        plt.xlabel(xlabel, fontsize=11)
    if ylabel:
        plt.ylabel(ylabel, fontsize=11)

    # Set y-axis limit if specified
    if ylim_max is not None:
        ax = plt.gca()
        ax.set_ylim(top=ylim_max)

    plt.legend(loc="best", fontsize=10)
    plt.grid(True, alpha=0.3)

    # Reduce number of ticks
    ax = plt.gca()
    ax.xaxis.set_major_locator(plt.MaxNLocator(nbins=8))
    ax.yaxis.set_major_locator(plt.MaxNLocator(nbins=6))

    plt.tight_layout()
    plt.show()


def plot_hourly_weekly(
    df_weekly,
    columns_to_plot,
    title="",
    xlabel="",
    ylabel="",
    color_list=None,
    linestyle_list=None,
    legend_labels=None,
    figsize=(14, 6),
    shade_hours=None,
):
    """
    Plot weekly time series profile (168 hours).

    Parameters:
    -----------
    df_weekly : pd.DataFrame
        DataFrame indexed by hour_of_week (0-167) with columns to plot
    columns_to_plot : list
        List of column names to plot
    title : str, optional
        Plot title
    xlabel : str, optional
        X-axis label
    ylabel : str, optional
        Y-axis label
    color_list : list, optional
        Colors for each column (default: uses first N from colors)
    linestyle_list : list, optional
        Line styles for each column (default: '-' for all)
    legend_labels : list, optional
        Legend labels for each column (default: column names)
    figsize : tuple, optional
        Figure size (width, height)
    shade_hours : tuple, optional
        (start_hour, end_hour) to shade each day, e.g., (7, 9) for 7am-9am
        Shades this time range for all 7 days
    """
    # Set defaults
    if color_list is None:
        color_list = COLORS[: len(columns_to_plot)]
    if linestyle_list is None:
        linestyle_list = ["-"] * len(columns_to_plot)
    if legend_labels is None:
        legend_labels = columns_to_plot

    plt.figure(figsize=figsize)

    # Plot each column
    for col, color, linestyle, label in zip(
        columns_to_plot, color_list, linestyle_list, legend_labels
    ):
        plt.plot(
            df_weekly.index,
            df_weekly[col],
            color=color,
            linewidth=2,
            linestyle=linestyle,
            label=label,
            alpha=0.7,
            marker="o",
            markersize=1.25,
        )

    # Add shaded regions if specified
    if shade_hours is not None:
        start_hour, end_hour = shade_hours
        ax = plt.gca()
        y_min, y_max = ax.get_ylim()

        for day in range(7):
            shade_start = day * 24 + start_hour
            shade_end = day * 24 + end_hour
            plt.axvspan(shade_start, shade_end, color="gray", alpha=0.15, zorder=0)

    # Add day boundaries
    for day in range(1, 7):
        plt.axvline(day * 24, color="gray", linestyle="--", alpha=0.2, linewidth=1)

    # Custom x-axis labels: Day + Hour
    ax = plt.gca()

    # Set major ticks at day boundaries (every 24 hours)
    major_ticks = [i * 24 for i in range(8)]
    ax.set_xticks(major_ticks)
    ax.set_xticklabels(
        ["Mon\n0h", "Tue\n0h", "Wed\n0h", "Thu\n0h", "Fri\n0h", "Sat\n0h", "Sun\n0h", "Mon\n0h"]
    )

    # Add minor ticks for hours (every 6 hours)
    minor_ticks = [i * 6 for i in range(29)]
    ax.set_xticks(minor_ticks, minor=True)
    ax.tick_params(axis="x", which="minor", length=3)

    if title:
        plt.title(title, fontsize=15)
    if xlabel:
        plt.xlabel(xlabel, fontsize=11)
    if ylabel:
        plt.ylabel(ylabel, fontsize=11)

    plt.legend(loc="best", fontsize=10)
    plt.grid(True, alpha=0.3, which="major")
    plt.grid(True, alpha=0.1, which="minor", linestyle=":")
    plt.tight_layout()
    plt.show()


def plot_points_on_map(
    df_points,
    lat_col="lat",
    lon_col="lon",
    label_col=None,
    group_col=None,
    group_values=None,
    color_list=None,
    marker_list=None,
    markersize_list=None,
    legend_labels=None,
    figsize=(12, 12),
    bbox=None,
    show_labels=True,
    title="Points on Map",
    zoom=12,
):
    """
    Plot points on map with flexible grouping and styling.

    Parameters:
    -----------
    df_points : pd.DataFrame
        DataFrame with point data
    lat_col : str
        Column name for latitude (default: 'lat')
    lon_col : str
        Column name for longitude (default: 'lon')
    label_col : str, optional
        Column name for point labels (default: None)
    group_col : str, optional
        Column name to group points by (default: None, all same group)
    group_values : list of lists
        List of value lists defining each group.
        Example: [['A', 'B'], ['C', 'D'], ['E']] creates 3 groups
        Must match length of color_list
    color_list : list
        Colors for each group. Must match length of group_values
    marker_list : list or str, optional
        Marker types. If single value, applies to all groups (default: 'o')
    markersize_list : list or int, optional
        Marker sizes. If single value, applies to all groups (default: 50)
    legend_labels : list, optional
        Custom labels for legend (default: 'Group 1', 'Group 2', etc.)
    figsize : tuple
        Figure size (width, height)
    bbox : tuple, optional
        Bounding box (min_lon, min_lat, max_lon, max_lat)
    show_labels : bool
        Whether to show point labels (only if <= 20 points)
    title : str
        Plot title
    zoom : int
        Basemap zoom level
    """
    df_plot = df_points.copy()

    # Filter by bounding box if provided
    if bbox is not None:
        min_lon, min_lat, max_lon, max_lat = bbox
        df_plot = df_plot[
            (df_plot[lon_col] >= min_lon)
            & (df_plot[lon_col] <= max_lon)
            & (df_plot[lat_col] >= min_lat)
            & (df_plot[lat_col] <= max_lat)
        ]

    # Handle grouping
    if group_col is None or group_values is None:
        # Single group - all points
        df_plot["_group"] = 0
        n_groups = 1
        if color_list is None:
            color_list = [COLORS[2]]
    else:
        # Assign groups based on group_values
        n_groups = len(group_values)
        df_plot["_group"] = -1  # Unassigned points get -1

        for group_idx, values in enumerate(group_values):
            mask = df_plot[group_col].isin(values)
            df_plot.loc[mask, "_group"] = group_idx

        # Filter out unassigned points
        df_plot = df_plot[df_plot["_group"] != -1]

        # Validate color_list matches group_values
        if color_list is None or len(color_list) != n_groups:
            raise ValueError(f"color_list must have {n_groups} colors to match group_values")

    # Handle markers - replicate if single value
    if marker_list is None:
        marker_list = ["o"] * n_groups
    elif isinstance(marker_list, str):
        marker_list = [marker_list] * n_groups
    elif len(marker_list) == 1:
        marker_list = marker_list * n_groups
    elif len(marker_list) != n_groups:
        raise ValueError(f"marker_list must have {n_groups} values or be a single value")

    # Handle marker sizes - replicate if single value
    if markersize_list is None:
        markersize_list = [50] * n_groups
    elif isinstance(markersize_list, (int, float)):
        markersize_list = [markersize_list] * n_groups
    elif len(markersize_list) == 1:
        markersize_list = markersize_list * n_groups
    elif len(markersize_list) != n_groups:
        raise ValueError(f"markersize_list must have {n_groups} values or be a single value")

    # Handle legend labels
    if legend_labels is None:
        legend_labels = [f"Group {i+1}" for i in range(n_groups)]
    elif len(legend_labels) != n_groups:
        raise ValueError(f"legend_labels must have {n_groups} labels")

    # Create GeoDataFrame
    geometry = [Point(xy) for xy in zip(df_plot[lon_col], df_plot[lat_col])]
    gdf = gpd.GeoDataFrame(df_plot, geometry=geometry, crs="EPSG:4326")
    gdf = gdf.to_crs(epsg=3857)

    # Create plot
    fig, ax = plt.subplots(figsize=figsize)

    # Plot each group
    for group_idx in range(n_groups):
        group_gdf = gdf[gdf["_group"] == group_idx]
        if len(group_gdf) > 0:
            group_gdf.plot(
                ax=ax,
                color=color_list[group_idx],
                marker=marker_list[group_idx],
                markersize=markersize_list[group_idx],
                alpha=0.7,
                edgecolor="black",
                linewidth=0.5,
                zorder=5,
                label=legend_labels[group_idx],
            )

    # Set map extent if bbox provided
    if bbox is not None:
        transformer = Transformer.from_crs("EPSG:4326", "EPSG:3857", always_xy=True)
        min_x, min_y = transformer.transform(min_lon, min_lat)
        max_x, max_y = transformer.transform(max_lon, max_lat)
        ax.set_xlim(min_x, max_x)
        ax.set_ylim(min_y, max_y)

    # Add basemap
    ctx.add_basemap(ax, source=ctx.providers.CartoDB.Positron, zoom=zoom)

    # Labels
    if show_labels and label_col is not None and len(df_plot) <= 20:
        for idx, row in gdf.iterrows():
            ax.annotate(
                row[label_col],
                xy=(row.geometry.x, row.geometry.y),
                xytext=(5, 5),
                textcoords="offset points",
                fontsize=8,
                alpha=0.8,
                bbox=dict(boxstyle="round,pad=0.3", facecolor="white", alpha=0.8),
                zorder=6,
            )

    ax.set_title(title, fontsize=15)
    if n_groups > 1:
        ax.legend(loc="upper right", fontsize=10)
    ax.axis("off")
    plt.tight_layout()
    plt.show()

    return fig, ax
