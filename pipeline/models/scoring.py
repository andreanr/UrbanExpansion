import sys
import pdb
import numpy as np
import pandas as pd
import statistics
from sklearn import metrics


def generate_binary_at_x(test_predictions, cutoff):
    test_predictions_binary = [1 if x >= cutoff else 0 for x in test_predictions]
    return test_predictions_binary


def confusion_matrix_at_x(test_labels, test_prediction_binary_at_x, costs):
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

    true_positive_u = [1 if x == 1 and y == 1 and z == 0 else 0
                       for (x, y, z) in zip(test_prediction_binary_at_x, test_labels, costs)]
    false_positive_u = [1 if x == 1 and y == 0 and z == 0 else 0
                     for (x, y, z) in zip(test_prediction_binary_at_x, test_labels, costs)]
    true_negative_u = [1 if x == 0 and y == 0 and z==0 else 0
                     for (x, y, z) in zip(test_prediction_binary_at_x, test_labels, costs)]
    false_negative_u = [1 if x == 0 and y == 1 and z==0 else 0
                     for (x, y, z) in zip(test_prediction_binary_at_x, test_labels, costs)]

    TP = np.sum(true_positive)
    TN = np.sum(true_negative)
    FP = np.sum(false_positive)
    FN = np.sum(false_negative)

    TP_u = np.sum(true_positive_u)
    TN_u = np.sum(true_negative_u)
    FP_u = np.sum(false_positive_u)
    FN_u = np.sum(false_negative_u)
    
    return TP, TN, FP, FN, TP_u, TN_u, FP_u, FN_u


def calculate_all_evaluation_metrics( test_label, test_predictions, costs):
    """ Calculate several evaluation metrics using sklearn for a set of
        labels and predictions.
    Args:
        test_labels (list): list of true labels for the test data.
        test_predictions (list): list of risk scores for the test data
    Returns:
        all_metrics (dict)
    """

    all_metrics = dict()

    cutoffs = [.1, .15, .2, .25, .3, .35, .4, .45, .5,
               .55, .6, .65, .7, .75, .8, .85, .9]
    for cutoff in cutoffs:
        test_predictions_binary_at_x = generate_binary_at_x(test_predictions, cutoff)
        TP, TN, FP, FN, TP_u, TN_u, FP_u, FN_u = confusion_matrix_at_x(test_label,
                                                                       test_predictions_binary_at_x,
                                                                       costs)
        all_metrics["true positives@|{}".format(str(cutoff))] = TP
        all_metrics["true negatives@|{}".format(str(cutoff))] = TN
        all_metrics["false positives@|{}".format(str(cutoff))] = FP
        all_metrics["false negatives@|{}".format(str(cutoff))] = FN
        # with costs
        all_metrics["true positives_u@|{}".format(str(cutoff))] = TP_u
        all_metrics["true negatives_u@|{}".format(str(cutoff))] = TN_u
        all_metrics["false positives_u@|{}".format(str(cutoff))] = FP_u
        all_metrics["false negatives_u@|{}".format(str(cutoff))] = FN_u

        # precision
        all_metrics["precision@|{}".format(str(cutoff))] = [TP / ((TP + FP) * 1.0) if (TP + FP) > 0 else 'Null'][0]
        # recall
        all_metrics["recall@|{}".format(str(cutoff))] = [TP / ((TP + FN) * 1.0) if (TP + FN)> 0 else 'Null'][0]
        # f1 score
        all_metrics["f1@|{}".format(str(cutoff))] = [(2* TP) / ((2*TP + FP + FN)*1.0) if (TP + FP + FN) > 0 else 'Null'][0]
        # accuracy
        all_metrics["auc@|{}".format(str(cutoff))] = [(TP + TN) / ((TP + TN + FP + FN)*1.0) if (TP + TN + FP + FN) > 0 else 'Null'][0]

        # precision
        all_metrics["precision_u@|{}".format(str(cutoff))] = [TP_u / ((TP_u + FP_u) * 1.0) if (TP_u + FP_u) > 0 else 'Null'][0]
        # recall
        all_metrics["recall_u@|{}".format(str(cutoff))] = [TP_u / ((TP_u + FN_u) * 1.0) if (TP_u + FN_u)> 0 else 'Null'][0]
        # f1 score
        all_metrics["f1_u@|{}".format(str(cutoff))] = [(2* TP_u) / ((2*TP_u + FP_u + FN_u)*1.0) if (TP_u + FP_u + FN_u) > 0 else 'Null'][0]
        # accuracy
        all_metrics["auc_u@|{}".format(str(cutoff))] = [(TP_u + TN_u) / ((TP_u + TN_u + FP_u + FN_u)*1.0) if (TP_u + TN_u + FP_u + FN_u) > 0 else 'Null'][0]

    return all_metrics

def cv_evaluation_metrics(fold_metrics):

    df_metrics = pd.DataFrame.from_dict(fold_metrics)
    metrics = df_metrics.T.mean().to_dict()
    return metrics
