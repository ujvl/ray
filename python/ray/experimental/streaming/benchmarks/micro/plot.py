from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
from itertools import cycle, groupby, islice, product, repeat
import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import sys
import uuid

from statistics import mean, median, stdev
from statsmodels.distributions.empirical_distribution import ECDF


NUM_PARAMETERS = 18  # Total number of parameters for experiments with the API
NUM_QUEUE_PARAMETERS = 16
API_QUEUE_COMMON_PARAMETERS = 14

queue_only_parameters = ["max_reads"]
not_queue_parameters = ["task_based", "dataflow_parallelism", "partitioning"]

# Parameters position in log file names
_index_of = {"nodes":0, "source_rate":1,
             "redis_shards":2, "redis_max_memory":3,
             "plasma_memory":4, "rounds":5, "sample_period":6,
             "record_type":7,"record_size":8,
             "max_queue_size":9,"max_batch_size":10,
             "batch_timeout":11,"prefetch_depth":12,
             "background_flush":13,"num_stages":14,
             "partitioning":15,"task_based":16,
             "dataflow_parallelism":17, "max_reads":15}

# Default file prefixes
latency_plot_file_prefix = "latency_plot_"
throughput_plot_file_prefix = "throughput_plot_"
latency_file_prefix = "latencies"
throughput_file_prefix = "throughputs"
dump_file_prefix = "dump_"

# Used to load parameters from a configuration file
class LoadFromFile(argparse.Action):
    def __call__ (self, parser, namespace, conf_file, option_string = None):
        args = []
        with conf_file as cf:
            for line in cf:
                line = line.strip()
                if line and line[0] != "#":
                    args.append(line)
        parser.parse_args(args, namespace)

parser = argparse.ArgumentParser()
# Plot-related parameteres
parser.add_argument("--plot-type", default="latency",
                    choices = ["api_overhead", "latency",
                               "throughput", "latency_vs_throughput"],
                    help="the number of batches prefetched from plasma")
parser.add_argument("--cdf", default=False,
                    action = 'store_true',
                    help="whether to generate latency CDFs instead of lines")
parser.add_argument("--logscale", default=False,
                    action = 'store_true',
                    help="whether to generate a logscale plot")
parser.add_argument("--plot-repo", default="./",
                    help="the folder to store plots")
parser.add_argument("--plot-file", default=latency_plot_file_prefix,
                    help="the plot file prefix")
# File-related parameteres
parser.add_argument("--file-repo", default="./",
                    help="the folder containing the log files")
parser.add_argument("--latency-file", default=latency_file_prefix,
                    help="the file containing per-record latencies")
parser.add_argument("--throughput-file", default=throughput_file_prefix,
                    help="the file containing actors throughput")
parser.add_argument("--dump-file", default=dump_file_prefix,
                    help="the chrome timeline dump file")
# Dataflow-related parameters
parser.add_argument("--nodes", default=1,
                    help="total number of nodes in the cluster")
parser.add_argument("--redis-shards", default=1,
                    help="total number of Redis shards")
parser.add_argument("--redis-max-memory", default=10**9,
                    help="max amount of memory per Redis shard")
parser.add_argument("--plasma-memory", default=10**9,
                    help="amount of memory to start plasma with")
parser.add_argument("--source-rate", default=-1,
                    type=lambda x: ((float(x) == -1) or float(x)) or
                                parser.error("Source rate cannot be zero."),
                    help="source output rate (records/s)")
parser.add_argument("--rounds", default=1,
                    help="the number of rounds in the experiment")
parser.add_argument("--num-stages", default=2,
                    help="the number of stages in the chain")
parser.add_argument("--dataflow-parallelism", default=1,
                    help="the number of instances per operator")
parser.add_argument("--partitioning", default="round_robin",
                    choices = ["round_robin", "shuffle", "broadcast"],
                    help="type of partitioning used after each stage")
parser.add_argument("--task-based", default="False",
                    help="task- or queue-based execution")
parser.add_argument("--record-type", default="int",
                    choices = ["int","string"],
                    help="the type of records used in the experiment")
parser.add_argument("--record-size", default=None,
                    help="the size of a record of type string in bytes")
parser.add_argument("--sample-period", default=100,
                    help="every how many input records latency was measured.")
# Queue-related parameters
parser.add_argument("--queue-size", default=100,
                    help="the queue size in number of batches")
parser.add_argument("--batch-size", default=1000,
                    help="the batch size in number of elements")
parser.add_argument("--batch-timeout", default=0.1,
                    help="the timeout to flush a batch")
parser.add_argument("--prefetch-depth", default=1,
                    help="the number of batches prefetched from plasma")
parser.add_argument("--background-flush", default=False,
                    choices = [True,False],
                    help="whether flush was done by a another thread or not")
parser.add_argument("--max-reads", default=float("inf"),
                    help="Maximum read throughput for batched queues")

parser.add_argument("--conf-file", type=open, action=LoadFromFile,
                    help="load parameters from a configuration file")

# 'agg' backend is used to create plot as a .png file
mpl.use('agg')

# Colors
reds = mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(vmin=-8, vmax=8),
                             cmap="Reds")
greens = mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(vmin=-8, vmax=8),
                               cmap="Greens")
blues = mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(vmin=-8, vmax=8),
                              cmap="Blues")
oranges = mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(vmin=-8, vmax=8),
                              cmap="Oranges")
greys = mpl.cm.ScalarMappable(norm=mpl.colors.Normalize(vmin=-8, vmax=8),
                              cmap="Greys")

# Line color generator
colors = cycle([blues, greys, reds, greens, oranges])

# Linestyle generator
linestyles = cycle(['solid', 'dashed', 'dotted', 'dashdot'])

# Generates plot UUIDs
def _generate_uuid():
    return uuid.uuid4()

# Parses an argument and returns its value(s)
def _parse_arg(arg, type="string"):
    arg = str(arg)
    content = [arg]
    # First check if a collection of values was given
    if arg[0] == "[" and arg[-1] == "]":
        content = arg[1:-1].split(",")  # Assume correct syntax
    elif type == "int":
        content = [int(arg)]
    elif type == "float":
        content = [float(arg)]
    elif type == "bool":
        content = [True] if arg == "True" or arg == "true" else [False]
    elif not type == "string":
        sys.exit("Unrecognized argument type: {}".format(arg))
    return content

# Collects all varying parameters and their values from a given configuration
# Returns a list of tuples of the form (parameter_tag, values)
def _varying_parameters(experiment_args):
    varying_parameters = []
    for tag, arg in experiment_args:
        if len(arg) > 1:  # A collection of values was given
            varying_parameters.append((tag,arg))
    return varying_parameters

# Collects all value combinations in the case of multiple varying parameters
# Returns a tuple of the form (combined_parameters_tag, value_combinations)
def _varying_combination(varying_parameters):
    combined_tag = "("
    combined_values = []
    for tag, values in varying_parameters:
        combined_tag += str(tag) + ", "
        combined_values.append(values)
    combined_tag = combined_tag[:-2] + ")"
    combined_values = list(product(*combined_values))
    return combined_tag, combined_values

# Generates line labels for the plot legend
def _generate_labels(parameter_names, parameter_values, label_prefix=""):
    labels = []
    label = label_prefix
    for parameter in parameter_names:
        label += parameter
    label += ": "
    for values in parameter_values:
        labels.append(label+str(values))
    return labels

# Checks if the combination of parameters used in an experiment with
# the Streaming API is the same as that of a plain-queue experiment
def _is_pair(api_parameters, queue_parameters):
    assert(len(queue_parameters) < len(api_parameters))
    for i in range(len(queue_parameters)):
        if i <= API_QUEUE_COMMON_PARAMETERS:
            if queue_parameters[i] != api_parameters[i]:
                return False
    return True

# Collects per-record latencies from log files
# Files are identified using exp_args
# Returns a list of tuples of the form (parameter_tag, latencies)
def collect_latencies(file_repo, latency_file_prefix,
                      exp_args, plain_queues=False):
    all_latencies = []  # One list of measured latencies per file
    size = NUM_QUEUE_PARAMETERS if plain_queues else NUM_PARAMETERS
    all_parameters = [[]] * size
    # Sort parameters in the order they appear in filenames
    max_index = -1
    for tag, values in exp_args:
        if plain_queues:
            if tag in not_queue_parameters:
                continue
        elif tag in queue_only_parameters:
            continue
        index = _index_of[tag]
        # print("Tag: {} Index: {}".format(tag,index))
        all_parameters[index] = values
        if max_index < index:
            max_index = index
    max_index += 1
    while max_index < size - 1:  # Resize
        all_parameters.pop()
        max_index += 1
    # print("Parameters: {}".format(all_parameters))
    # Get all parameter combinations
    all_combinations = list(product(*all_parameters))
    # Read all necessary latency files
    if file_repo[-1] != "/":
        file_repo += "/"
    filename_prefix = file_repo + latency_file_prefix
    for parameters_combination in all_combinations:
        latencies = []
        filename = filename_prefix
        for parameter_value in parameters_combination:
            filename += "-" + str(parameter_value)
        print("Reading file: {}".format(filename))
        try:  # Read latencies
            with open(filename,"r") as lf:
                for latency in lf:
                    latencies.append(float(latency))
                all_latencies.append((parameters_combination, latencies))
        except FileNotFoundError:
            sys.exit("Could not find file '{}'".format(filename))
    return all_latencies

# Collects actor input/output rates from all necessary files
# Files are identified using exp_args
# Returns a list of tuples of the form (parameter_tag, rates),
# where rates = (mean input rate, stdev, mean output rate, stdev)
def collect_rates(file_repo, throughput_file_prefix,
                      exp_args, plain_queues=False):
    all_rates = []  # One list of measured rates per file
    size = NUM_QUEUE_PARAMETERS if plain_queues else NUM_PARAMETERS
    all_parameters = [[]] * size
    # Sort parameters in the order they appear in filenames
    max_index = -1
    for tag, values in exp_args:
        if plain_queues:
            if tag in not_queue_parameters:
                continue
        elif tag in queue_only_parameters:
            continue
        index = _index_of[tag]
        # print("Tag: {} Index: {}".format(tag,index))
        all_parameters[index] = values
        if max_index < index:
            max_index = index
    max_index += 1
    while max_index < size - 1:  # Resize
        all_parameters.pop()
        max_index += 1
    # print("Parameters: {}".format(all_parameters))
    # Get all parameter combinations
    all_combinations = list(product(*all_parameters))
    # Read all necessary latency files
    if file_repo[-1] != "/":
        file_repo += "/"
    filename_prefix = file_repo + throughput_file_prefix
    for parameters_combination in all_combinations:
        # actor id -> ([in_rate], [out_rate])
        raw_rates = {}
        # actor id -> (median in_rate, variance, median out_rate, variance)
        rates = []
        filename = filename_prefix
        for parameter_value in parameters_combination:
            filename += "-" + str(parameter_value)
        print("Reading file: {}".format(filename))
        try:  # Read rates
            with open(filename,"r") as lf:
                for rate_log in lf:
                    # actor_id | in_rate | out_rate
                    log = rate_log.strip().split("|")
                    log = [element.strip() for element in log]
                    assert(len(log) >= 2)
                    if len(log) < 3:  # In case of plain-queue experiments
                        log.append(log[1])  # set in_rate = out_rate
                    entry = raw_rates.setdefault(log[0],([],[]))
                    entry[0].append(float(log[1]))
                    entry[1].append(float(log[2]))
                # Compute mean rates and variance for each actor
                for actor_id, (in_rate, out_rate) in raw_rates.items():
                    if len(in_rate) > 1:
                        in_mean = median(in_rate)
                        in_stdev = 0.0 #stdev(in_rate,in_mean)
                    else:
                        in_mean = float(in_rate[0])
                        in_stdev = 0.0
                    if len(out_rate) > 1:
                        out_mean = median(out_rate)
                        out_stdev = 0.0 #stdev(out_rate,out_mean)
                    else:
                        out_mean = float(out_rate[0])
                        out_stdev = 0.0
                    rates.append((actor_id, in_mean, in_stdev,
                                  out_mean, out_stdev))
            all_rates.append((parameters_combination, rates))
        except FileNotFoundError:
            sys.exit("Could not find file '{}'".format(filename))
    return all_rates

# Generates a line plot
def generate_line_plot(x_data, y_data, x_label, y_label, line_labels,
                       plot_repo, plot_file_name,
                       cdf=False, point_labels=None, logscale=False):
    # Create a figure instance
    line_plot = plt.figure(1, figsize=(9, 6))
    # Create an axes instance
    ax = line_plot.add_subplot(111)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()
    i = 0
    if cdf:  # Generate CDF
        for data_x in x_data:
            label = line_labels[i]
            cdf = ECDF(data_x)
            ax.plot(cdf.x, cdf.y, label=label,
                    linestyle=next(linestyles), alpha=0.5,
                    linewidth=1, c=next(colors).to_rgba(1))
            i += 1
    else:  # Generate line plot
        for data_x in x_data:
            label = line_labels[i]
            data_y = y_data[i]
            ax.plot(data_x, data_y, label=label,
                    linestyle=next(linestyles), alpha=0.5,
                    linewidth=1, c=next(colors).to_rgba(1))
            i += 1
    ax.legend(fontsize=8)
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()
    if logscale:
        ax.set_yscale('log')
    # Set font sizes
    for item in ([ax.title, ax.xaxis.label,
        ax.yaxis.label] + ax.get_xticklabels() + ax.get_yticklabels()):
        item.set_fontsize(16)
    if point_labels is not None:  # Annotate data points
        assert not cdf  # Cannot annotate CDF points
        for data_x, data_y in zip(x_data, y_data):
            for x,y,label in zip(data_x,data_y,point_labels):
                ax.annotate(label,xy=(x,y))
    # Save plot
    if plot_repo[-1] != "/":
        plot_repo += "/"
    line_plot.savefig(
        plot_repo + "/" + plot_file_name,
        bbox_inches='tight')
    line_plot.clf()

# Generates a boxplot
def generate_box_plot(data, x_label, y_label,
                      labels, plot_repo, plot_file_name, logscale=False):
    # Create a figure instance
    box_plot = plt.figure(1, figsize=(9, 6))
    # Create an axes instance
    ax = box_plot.add_subplot(111)
    ax.set_xlabel(x_label)
    ax.set_ylabel(y_label)
    ax.get_xaxis().tick_bottom()
    ax.get_yaxis().tick_left()
    ax.boxplot(data)
    if logscale:
        ax.set_yscale('log')
    ax.set_xticklabels(labels)
    # Set font sizes
    for item in ([ax.title, ax.xaxis.label, ax.yaxis.label]):
        item.set_fontsize(14)
    for item in (ax.get_xticklabels() + ax.get_yticklabels()):
        item.set_fontsize(10)
    # Save plot
    if plot_repo[-1] != "/":
        plot_repo += "/"
    box_plot.savefig(
        plot_repo + "/" + plot_file_name,
        bbox_inches='tight')
    box_plot.clf()

# Generates a barchart
def generate_barchart_plot(data, x_label, y_label, bar_metadata, exp_labels,
                           plot_repo, plot_file_name, logscale=False):
    # TODO (john): Add option to use actor ids at the bottom of each bar
    num_exps = len(data)        # Total number of histograms in the plot
    ind = np.arange(num_exps)   # The x-axis locations of the histograms
    bar_width = 1               # The width of each bar in a histogram
    bar_plot, ax = plt.subplots()
    pos = 0
    step = bar_width
    for histogram, histogram_metadata in zip(data, bar_metadata):
        for value, value_metadata in zip(histogram, histogram_metadata):
            label = value_metadata[0] if value_metadata[3] else ""
            color = value_metadata[1] if value_metadata[1] else next(
                                                            colors).to_rgba(1)
            bar = ax.bar(pos + bar_width,
                           value,
                           label=label,
                           color=color,
                           alpha=0.5,
                           width=bar_width,
                           yerr=0)
            if value_metadata[2]:  # Hatch bar
                bar[0].set_hatch("/")
            pos += step
        pos += step  # Separate histograms by one bar length
    ax.set_ylabel(x_label)
    ax.set_ylabel(y_label)
    ax.legend(fontsize=12)
    x_ticks_positions = []
    offset = 0
    for histogram in data:
        group_width = len(histogram) * bar_width
        point = group_width / 2 + bar_width / 2
        pos = offset + point
        offset += group_width + step
        x_ticks_positions.append(pos)
    ax.set_xticks(x_ticks_positions)
    # Generate short a-axis tick labels
    short_xticks = ["Exp"+str(i) for i in range(len(exp_labels))]
    ax.set_xticklabels(tuple(short_xticks))
    # Move actual x-axis tick labels outside the plot
    labels = ""
    i = 0
    for label in exp_labels:
        labels += "Exp" + str(i) + " - " + label + "\n"
        i += 1
    ax.text(1.05, 0.95, labels, transform=ax.transAxes, fontsize=10,
                                verticalalignment='top')
    # Set font size for x-axis tick labels
    for item in ax.get_xticklabels():
        item.set_fontsize(10)
    if logscale:
        ax.set_yscale('log')
    # Save plot
    if plot_repo[-1] != "/":
        plot_repo += "/"
    bar_plot.savefig(
        plot_repo + "/" + plot_file_name,
        bbox_inches='tight')
    bar_plot.clf()

# Generates a latency vs throughput plot for a single varying parameter
def latency_vs_throughput(latencies, rates, plot_repo, varying_parameters,
                          plot_file_prefix, plot_type="line", logscale=False):
    assert len(latencies) > 0
    assert len(rates) > 0
    # TODO (john): Support multiple varying parameters
    assert len(varying_parameters) == 1
    if plot_type == "line":
        labels = []
        tag, values = varying_parameters[0]
        labels.append(str(tag) + ": " + str(values))
        x_axis_label = "Latency [ms]"
        y_axis_label = "Throughput [records/s]"
        plot_filename = plot_file_prefix + "-latency-vs-throughput.png"
        generate_line_plot([latencies], [rates], x_axis_label, y_axis_label,
                           labels, plot_repo, plot_filename,
                           plot_type=="cdf", values, logscale=logscale)
    else:
        sys.exit("Unrecognized or unsupported plot type.")

# Generates boxplots showing the overhead of
# using the Streaming API over batch queues
def api_overhead(latencies, plot_repo, varying_parameters,
                 plot_file_prefix, plot_type="boxplot", logscale=False):
    assert len(latencies) > 0
    labels = [""] * len(latencies)
    try:  # In case there is at least one varying parameter
        tag, values = varying_parameters
        if len(values) > 0:
            for i in range(0, len(labels) - 1, 2):
                value_str = str(values[i // 2])
                labels[i] = "(" + tag + ": " + value_str + ") - Streaming API"
                labels[i+1] = "(" + tag + ": " + value_str + ") - Plain Queue"
    except TypeError: # All parameters are fixed
        for i in range(0, len(labels) - 1, 2):
            labels[i] = "Streaming API"
            labels[i+1] = "Plain Queue"
    if plot_type == "boxplot":
        x_axis_label = ""
        y_axis_label = "End-to-end record latency [s]"
        plot_filename = plot_file_prefix + "-api-overhead.png"
        data = [data for _, data in latencies]
        generate_box_plot(data, x_axis_label, y_axis_label,
                          labels, plot_repo, plot_filename, logscale)
    elif plot_type == "cdf":
        x_axis_label = "End-to-end record latency [s]"
        y_axis_label = "Percentage [%]"
        plot_filename = plot_file_prefix + "-api-overhead-cdf.png"
        x_data = [data for _, data in latencies]
        generate_line_plot(x_data, [], x_axis_label, y_axis_label,
                          labels, plot_repo, plot_filename, True,
                          logscale=logscale)
    else:
        sys.exit("Unrecognized or unsupported plot type.")

# Generates barcharts showing actor rates
def actor_rates(rates, plot_repo, varying_parameters,
                 plot_file_prefix, plot_type="barchart", logscale=False):
    assert len(rates) > 0
    try:
        tag, values = varying_parameters
        exp_labels = _generate_labels(tag, values)
    except TypeError:  # All parameteres are fixed
        exp_labels = []
    if plot_type == "barchart":
        x_axis_label = ""
        y_axis_label = "Actor processing rate [records/s]"
        plot_filename = plot_file_prefix + "-actor-rates.png"
        # Prepare data for barchart
        data = []
        bar_metadata = []
        i = 0
        label_is_set = False  # Add one plot label to indicate the source bars
        for _, actor_rates in rates:
            data.append([])
            bar_metadata.append([])
            for actor_id, in_rate, _, out_rate, _ in actor_rates:
                hatch = False
                add_label = False
                color = ""
                actor_name = actor_id.split(",")[1].strip()
                if actor_name == "flatmap" or actor_name == "sink":
                    continue
                elif actor_name == "source":
                    data[i].append(out_rate)
                    hatch = True
                    color = "rosybrown"
                    if not label_is_set:
                        add_label = True
                        label_is_set = True
                else:
                    assert "map_" in actor_name
                    # 'in_rate' must be equal to 'out_rate' for identity maps
                    # We use 'out_rate' because the current logging for task-
                    # based execution does not log 'in_rate'. In queue-based
                    # execution both 'in_rate' and 'out_rate' are logged
                    data[i].append(out_rate)
                bar_metadata[i].append((actor_name,color,hatch,add_label))
            i += 1
        generate_barchart_plot(data, x_axis_label, y_axis_label,
                               bar_metadata, exp_labels, plot_repo,
                               plot_filename, logscale)
    else:
        sys.exit("Unrecognized or unsupported plot type.")

# Generates plots showing how end-to-end record latency is affected
# by varying the batched queue size
def latency_vs_queue_size(latencies, plot_repo, parameter_values,
                          plot_file_prefix, plot_type, logscale=False):
    labels = _generate_labels([], parameter_values,
                              label_prefix="Queue size")
    samples = []
    if plot_type == "line":
        x_axis_label = "Samples"
        y_axis_label = "End-to-end record latency [s]"
        plot_filename = plot_file_prefix + "-queue-sizes.png"
    elif plot_type == "cdf":
        x_axis_label = "End-to-end record latency [s]"
        y_axis_label = "Percentage [%]"
        plot_filename = plot_file_prefix + "-queue-sizes-cdf.png"
    else:
        sys.exit("Unrecognized or unsupported plot type.")

    x_data = [data for _, data in latencies] if plot_type=="cdf" else [
                              [s for s in range(len(data[1]))]
                              for data in latencies]
    y_data = [] if plot_type=="cdf" else [data for _, data in latencies]
    generate_line_plot(x_data, y_data, x_axis_label, y_axis_label,
                       labels, plot_repo, plot_filename,
                       plot_type=="cdf", logscale=logscale)

# Generates plots showing how end-to-end record latency is affected
# by varying the batch size
def latency_vs_batch_size(latencies, plot_repo, parameter_values,
                          plot_file_prefix, plot_type="line", logscale=False):
    labels = _generate_labels([], parameter_values,
                              label_prefix="Batch size")
    if plot_type == "line":
        x_axis_label = "Samples"
        y_axis_label = "End-to-end record latency [s]"
        plot_filename = plot_file_prefix + "-batch-sizes.png"
    elif plot_type == "cdf":
        x_axis_label = "End-to-end record latency [s]"
        y_axis_label = "Percentage [%]"
        plot_filename = plot_file_prefix + "-batch-sizes-cdf.png"
    else:
        sys.exit("Unrecognized or unsupported plot type.")

    x_data = [data for _, data in latencies] if plot_type=="cdf" else [
                              [s for s in range(len(data[1]))]
                              for data in latencies]
    y_data = [] if plot_type=="cdf" else [data for _, data in latencies]
    generate_line_plot(x_data, y_data, x_axis_label, y_axis_label,
                       labels, plot_repo, plot_filename,
                       plot_type=="cdf", logscale=logscale)

# Generates plots showing how end-to-end record latency is affected
# by varying the batch timeout (flush timeout)
def latency_vs_timeout(latencies, plot_repo, parameter_values,
                       plot_file_prefix, plot_type="line", logscale=False):
    labels = _generate_labels([], parameter_values,
                              label_prefix="Batch timeout")
    if plot_type == "line":
        x_axis_label = "Samples"
        y_axis_label = "End-to-end record latency [s]"
        plot_filename = plot_file_prefix + "-timeouts.png"
    elif plot_type == "cdf":
        x_axis_label = "End-to-end record latency [s]"
        y_axis_label = "Percentage [%]"
        plot_filename = plot_file_prefix + "-timeouts-cdf.png"
    else:
        sys.exit("Unrecognized or unsupported plot type.")

    x_data = [data for _, data in latencies] if plot_type=="cdf" else [
                              [s for s in range(len(data[1]))]
                              for data in latencies]
    y_data = [] if plot_type=="cdf" else [data for _, data in latencies]
    generate_line_plot(x_data, y_data, x_axis_label, y_axis_label,
                       labels, plot_repo, plot_filename,
                       plot_type=="cdf", logscale=logscale)

# Generates plots showing how end-to-end record latency is affected
# when using tasks compared to batched queues
def latency_task_vs_queue(latencies, plot_repo, parameter_values,
                          plot_file_prefix, plot_type="line", logscale=False):
    labels = _generate_labels([], parameter_values,
                              label_prefix="Task-based")
    if plot_type == "line":
        x_axis_label = "Samples"
        y_axis_label = "End-to-end record latency [s]"
        plot_filename = plot_file_prefix + "-task-vs-queue.png"
    elif plot_type == "cdf":
        x_axis_label = "End-to-end record latency [s]"
        y_axis_label = "Percentage [%]"
        plot_filename = plot_file_prefix + "-task-vs-queue-cdf.png"
    else:
        sys.exit("Unrecognized or unsupported plot type.")

    x_data = [data for _, data in latencies] if plot_type=="cdf" else [
                              [s for s in range(len(data[1]))]
                              for data in latencies]
    y_data = [] if plot_type=="cdf" else [data for _, data in latencies]
    generate_line_plot(x_data, y_data, x_axis_label, y_axis_label,
                       labels, plot_repo, plot_filename,
                       plot_type=="cdf", logscale=logscale)

# Generates plots showing how end-to-end record latency is affected
# by varying the number of stages in the dataflow
def latency_vs_chain_length(latencies, plot_repo, parameter_values,
                             plot_file_prefix, plot_type="line",
                             logscale=False):
    labels = _generate_labels([], parameter_values,
                              label_prefix="Chain length")
    if plot_type == "line":
        x_axis_label = "Samples"
        y_axis_label = "End-to-end record latency [s]"
        plot_filename = plot_file_prefix + "-chain-length.png"
    elif plot_type == "cdf":
        x_axis_label = "End-to-end record latency [s]"
        y_axis_label = "Percentage [%]"
        plot_filename = plot_file_prefix + "-chain-length-cdf.png"
    else:
        sys.exit("Unrecognized or unsupported plot type.")

    x_data = [data for _, data in latencies] if plot_type=="cdf" else [
                              [s for s in range(len(data[1]))]
                              for data in latencies]
    y_data = [] if plot_type=="cdf" else [data for _, data in latencies]
    generate_line_plot(x_data, y_data, x_axis_label, y_axis_label,
                           labels, plot_repo, plot_filename,
                           plot_type=="cdf", logscale=logscale)

# Generates plots showing how end-to-end record latency is affected
# by varying the number of operator instances per stage
def latency_vs_parallelism(latencies, plot_repo, parameter_values,
                             plot_file_prefix, plot_type="line",
                             logscale=False):
    labels = _generate_labels([], parameter_values,
                              label_prefix="Parallelism")
    if plot_type == "line":
        x_axis_label = "Samples"
        y_axis_label = "End-to-end record latency [s]"
        plot_filename = plot_file_prefix + "-parallelism.png"
    elif plot_type == "cdf":
        x_axis_label = "End-to-end record latency [s]"
        y_axis_label = "Percentage [%]"
        plot_filename = plot_file_prefix + "-parallelism-cdf.png"
    else:
        sys.exit("Unrecognized or unsupported plot type.")

    x_data = [data for _, data in latencies] if plot_type=="cdf" else [
                              [s for s in range(len(data[1]))]
                              for data in latencies]
    y_data = [] if plot_type=="cdf" else [data for _, data in latencies]
    generate_line_plot(x_data, y_data, x_axis_label, y_axis_label,
                           labels, plot_repo, plot_filename,
                           plot_type=="cdf", logscale=logscale)

# Generates plots showing how end-to-end record latency is affected
# by varying a combination of parameters
def latency_vs_multiple_parameters(latencies, plot_repo, varying_combination,
                                   plot_file_prefix, plot_type="line",
                                   logscale=False):
    combined_names = varying_combination[0]
    combined_values = varying_combination[1]
    labels = _generate_labels([combined_names],
                              combined_values)
    if plot_type == "line":
        x_axis_label = "Samples"
        y_axis_label = "End-to-end record latency [s]"
        plot_filename = plot_file_prefix + "-" + combined_names + ".png"
    elif plot_type == "cdf":
        x_axis_label = "End-to-end record latency [s]"
        y_axis_label = "Percentage [%]"
        plot_filename = plot_file_prefix + "-" + combined_names + "-cdf.png"
    else:
        sys.exit("Unrecognized or unsupported plot type.")

    x_data = [data for _, data in latencies] if plot_type=="cdf" else [
                              [s for s in range(len(data[1]))]
                              for data in latencies]
    y_data = [] if plot_type=="cdf" else [data for _, data in latencies]
    generate_line_plot(x_data, y_data, x_axis_label, y_axis_label,
                           labels, plot_repo, plot_filename,
                           plot_type=="cdf", logscale=logscale)

# Writes a log file containing all parameters
# of the experiment the plot corresponds to
def write_plot_metadata(plot_repo, plot_file_prefix,
                        plot_id, experiment_args):
    if plot_repo[-1] != "/":
        plot_repo += "/"
    filename = plot_repo + plot_file_prefix + plot_id + ".log"
    with open(filename,"w") as mf:
        for arg in experiment_args:
            tag, values = arg
            mf.write(tag + ": ")
            if len(values) > 1:  # A collection of values is given
                mf.write(str(values))
            else:  # A single value is given
                mf.write(str(values[0]))
            mf.write("\n")


if __name__ == "__main__":

    args = parser.parse_args()

    # Plotting arguments
    plot_type = str(args.plot_type)
    cdf = bool(args.cdf)
    logscale = bool(args.logscale)
    plot_repo = str(args.plot_repo)
    plot_file_prefix = str(args.plot_file)
    if plot_type == "throughput":
        plot_file_prefix = throughput_plot_file_prefix

    # Input file arguments
    file_repo = str(args.file_repo)
    latency_file_prefix = str(args.latency_file)
    throughput_file_prefix = str(args.throughput_file)
    dump_filename = str(args.dump_file)

    # Configuration arguments
    exp_args = []
    exp_args.append(("nodes", [int(args.nodes)]))
    input_rate = float(args.source_rate)
    if input_rate < 0:
        input_rate = "inf"
    exp_args.append(("source_rate", [input_rate]))
    exp_args.append(("redis_shards", [int(args.redis_shards)]))
    exp_args.append(("redis_max_memory", [int(args.redis_max_memory)]))
    exp_args.append(("plasma_memory", [int(args.plasma_memory)]))
    exp_args.append(("rounds", [int(args.rounds)]))
    exp_args.append(("num_stages",
                         _parse_arg(args.num_stages, type="int")))
    exp_args.append(("dataflow_parallelism",
                         _parse_arg(args.dataflow_parallelism, type="int")))
    exp_args.append(("partitioning", _parse_arg(args.partitioning)))
    exp_args.append(("task_based",
                         _parse_arg(args.task_based, type="bool")))
    exp_args.append(("record_type", _parse_arg(args.record_type)))
    exp_args.append(("record_size", _parse_arg(args.record_size)))
    exp_args.append(("sample_period",
                         _parse_arg(args.sample_period, type="int")))

    exp_args.append(("max_queue_size",
                         _parse_arg(args.queue_size, type="int")))
    exp_args.append(("max_batch_size",
                         _parse_arg(args.batch_size, type="int")))
    exp_args.append(("batch_timeout",
                          _parse_arg(args.batch_timeout, type="float")))
    exp_args.append(("prefetch_depth",
                         _parse_arg(args.prefetch_depth, type="int")))
    exp_args.append(("background_flush",
                         _parse_arg(args.background_flush, type="bool")))
    exp_args.append(("max_reads",
                         _parse_arg(args.max_reads, type="float")))

    plot_id = str(_generate_uuid())
    plot_file_prefix += plot_id

    # Sort experiment arguments on the order they appear in the log file name
    # This ensures that labels in plots are aligned with data read with
    # collect_latencies() and collect_rates() methods
    exp_args = sorted(exp_args, key=lambda element: _index_of[element[0]])
    # All parameters for which a collection of values is given
    varying_parameters = _varying_parameters(exp_args)

    # It is a latency vs throughput plot
    if plot_type == "latency_vs_throughput":
        if cdf:
            sys.exit("CDF are not supported for throughput plots.")
        if len(varying_parameters) == 0:
            sys.exit("No varying parameter found (at least one is needed).")
        plot_type = "line"
        # Collect latencies from log files
        latencies = collect_latencies(file_repo, latency_file_prefix,
                                      exp_args)
        median_latencies = []
        for tag, exp_latencies in latencies:
            median_latencies.append(median(exp_latencies))
        # Collect source rates from log files
        rates = collect_rates(file_repo, throughput_file_prefix, exp_args)
        median_rates = []
        source_rates = []
        for tag, actor_rates in rates:
            for _, in_rate, _, out_rate, _  in actor_rates:
                if in_rate == 0:  # It should be the source
                    source_rates.append(out_rate)
            median_rates.append(median(source_rates))
        latency_vs_throughput(median_latencies, median_rates,
                              plot_repo, varying_parameters, plot_file_prefix,
                              plot_type)
    elif plot_type == "throughput":  # It is a throughput plot
        if cdf:
            sys.exit("CDF are not supported for throughput plots.")
        plot_type = "barchart"
        # Collect rates from log files
        rates = collect_rates(file_repo,throughput_file_prefix,exp_args)
        varying = _varying_combination(
                  varying_parameters) if len(
                  varying_parameters) > 1 else varying_parameters[
                  0] if len(varying_parameters) > 0 else None
        actor_rates(rates, plot_repo, varying, plot_file_prefix, plot_type)
    else:  # It is a latency plot
        # Collect latencies from log files
        latencies = collect_latencies(file_repo,latency_file_prefix,exp_args)
        if plot_type == "api_overhead":  # Streaming API vs plain queues
            plot_type = "cdf" if cdf else "boxplot"
            # Collect latencies from the corresponding plain-queue experiments
            queue_latencies = collect_latencies(file_repo,latency_file_prefix,
                                                exp_args, plain_queues=True)
            # Pair up latencies so that they appear side-by-side
            paired_latencies = []
            for parameters, values in latencies:
                paired_latencies.append((parameters, values))
                for queue_parameters, queue_values in queue_latencies:
                    if _is_pair(parameters, queue_parameters):
                        paired_latencies.append((queue_parameters,
                                                 queue_values))
                        break
            varying = _varying_combination(
                      varying_parameters) if len(
                      varying_parameters) > 1 else varying_parameters[
                      0] if len(varying_parameters) > 0 else None
            api_overhead(paired_latencies, plot_repo, varying,
                         plot_file_prefix, plot_type, logscale)
        elif len(varying_parameters) > 1:  # Multiple varying parameters
            plot_type = "cdf" if cdf else "line"
            varying_combination = _varying_combination(varying_parameters)
            latency_vs_multiple_parameters(latencies, plot_repo,
                                           varying_combination,
                                           plot_file_prefix,
                                           plot_type, logscale)
        elif len(varying_parameters) == 1:
            # Identify parameter to generate the respective plot labels
            plot_type = "cdf" if cdf else "line"
            for tag, values in varying_parameters:
                if tag == "max_queue_size":
                    latency_vs_queue_size(latencies, plot_repo, values,
                                          plot_file_prefix, plot_type,
                                          logscale)
                elif tag == "max_batch_size":
                    latency_vs_batch_size(latencies, plot_repo, values,
                                          plot_file_prefix, plot_type,
                                          logscale)
                elif tag == "batch_timeout":
                    latency_vs_timeout(latencies, plot_repo, values,
                                       plot_file_prefix, plot_type,
                                       logscale)
                elif tag == "task_based":
                    latency_task_vs_queue(latencies, plot_repo, values,
                                          plot_file_prefix, plot_type,
                                          logscale)
                elif tag == "num_stages":
                    latency_vs_chain_length(latencies, plot_repo, values,
                                            plot_file_prefix, plot_type,
                                            logscale)
                elif tag == "dataflow_parallelism":
                    latency_vs_parallelism(latencies, plot_repo, values,
                                            plot_file_prefix, plot_type,
                                            logscale)
                else:
                    sys.exit("Unrecognized or unsupported option.")
        else:  # All parameters are fixed
            x_label = "Samples"
            y_label = "End-to-end record latency [s]"
            labels = [""]
            x_data = [data for _, data in latencies] if cdf else [
                                      [s for s in range(len(data[1]))]
                                      for data in latencies]
            y_data = [] if cdf else [data for _, data in latencies]
            generate_line_plot(x_data, y_data, x_label, y_label, labels,
                               plot_repo, plot_file_prefix, cdf,
                               logscale=logscale)
    # Store plot metadata
    write_plot_metadata(plot_repo, plot_file_prefix, plot_id, exp_args)
