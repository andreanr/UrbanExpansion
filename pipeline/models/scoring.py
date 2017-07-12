import sys
import pdb
import numpy as np
import pandas as pd
import statistics
from sklearn import metrics


def compute_AUC(test_labels, test_predictions):
    fpr, tpr, thresholds = metrics.roc_curve(
        test_labels, test_predictions, pos_label=1)
    return metrics.auc(fpr, tpr)


def compute_avg_false_positive_rate(test_labels, test_predictions):
    fpr, tpr, thresholds = metrics.roc_curve(
        test_labels, test_predictions, pos_label=1)
    return statistics.mean(fpr)


def compute_avg_true_positive_rate(test_labels, test_predictions):
    fpr, tpr, thresholds = metrics.roc_curve(
        test_labels, test_predictions, pos_label=1)

    return statistics.mean(tpr)

def generate_binary_at_x(test_predictions):
    test_predictions_binary = [1 if x < cutoff else 0 for x in test_predictions]
    return test_predictions_binary


def precision_at_x(test_labels, test_prediction_binary_at_x):
    """Return the precision at a specified percent cutoff
    Args:
        test_labels: ground truth labels for the predicted data
        test_predictions: prediction labels
        x_proportion: the percent of the prediction population to label. Must be between 0 and 1.
    """
    precision, _, _, _ = metrics.precision_recall_fscore_support(
        test_labels, test_prediction_binary_at_x)
    precision = precision[1]  # only interested in precision for label 1

    return precision

def recall_at_x(test_labels, test_prediction_binary_at_x):
    _, recall, _, _ = metrics.precision_recall_fscore_support(
        test_labels,test_prediction_binary_at_x)
    recall = recall[1]  # only interested in precision for label 1

    return recall

def confusion_matrix_at_x(test_labels, test_prediction_binary_at_x):
    """
    Returns the raw number of a given metric:
        'TP' = true positives,
        'TN' = true negatives,
        'FP' = false positives,
        'FN' = false negatives
    """

    # compute true and false positives and negatives.
    true_positive = [1 if x == 1 and y == 1 else 0 for (x, y) in zip(test_prediction_binary_at_x, test_labels)]
    false_positive = [1 if x == 1 and y == 0 else 0 for (x, y) in zip(test_prediction_binary_at_x, test_labels)]
    true_negative = [1 if x == 0 and y == 0 else 0 for (x, y) in zip(test_prediction_binary_at_x, test_labels)]
    false_negative = [1 if x == 0 and y == 1 else 0 for (x, y) in zip(test_prediction_binary_at_x, test_labels)]

    TP = np.sum(true_positive)
    TN = np.sum(true_negative)
    FP = np.sum(false_positive)
    FN = np.sum(false_negative)

    return TP, TN, FP, FN


def calculate_all_evaluation_metrics( test_label, test_predictions):
   """ Calculate several evaluation metrics using sklearn for a set of
        labels and predictions.
    :param list test_labels: list of true labels for the test data.
    :param list test_predictions: list of risk scores for the test data.
    :return: all_metrics
    :rtype: dict
    """

    all_metrics = dict()

    # FORMAT FOR DICTIONARY KEY
    # all_metrics["metric|parameter|unit|comment"] OR
    # all_metrics["metric|parameter|unit"] OR
    # all_metrics["metric||comment"] OR
    # all_metrics["metric"]

    cutoffs = [.1, .15, .2, .25, .3, .35, .4, .45, .5, .55,  .6,
               .65, .7, .75, .8, .85, .9]
    for cutoff in cutoffs:
        test_predictions_binary_at_x = generate_binary_at_x(test_predictions, cutoff)
        # precision
        all_metrics["precision@|{}".format(str(cutoff))] = precision_at_x(test_label, test_predictions_binary_at_x)
        # recall
        all_metrics["recall@|{}".format(str(cutoff)] = recall_at_x(test_label, test_predictions_binary_at_x)
        # confusion matrix
        TP, TN, FP, FN = confusion_matrix_at_x(test_label,  test_predictions_binary_at_x)
        all_metrics["true positives@|{}".format(str(cutoff))] = TP
        all_metrics["true negatives@|{}".format(str(cutoff))] = TN
        all_metrics["false positives@|{}".format(str(cutoff))] = FP
        all_metrics["false negatives@|{}".format(str(cutoff))] = FN
    return all_metrics


