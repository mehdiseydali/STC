import scapy.all as scapy
import os
def label_packets(pcap_file, label):
    """Labels the packets in a PCAP file based on the given label.

    Args:
        pcap_file: The path to the PCAP file.
        label: The label to assign to the packets.

    Returns:
        A list of labeled packets.
    """

    packets = scapy.rdpcap(pcap_file)
    for packet in packets:
        packet.options.append(scapy.Raw(load=label))

    return packets
def do_packet_labeling(dataset_path):
    """Labels the packets in a dataset of PCAP files based on the file names.

    Args:
        dataset_path: The path to the dataset directory containing the PCAP files.
    """

    for pcap_file in os.listdir(dataset_path):
        pcap_file_path = os.path.join(dataset_path, pcap_file)
        label = pcap_file.split(".")[0]

        labeled_packets = label_packets(pcap_file_path, label)

        # Save the labeled packets to a new file.
        new_pcap_file_path = os.path.join(dataset_path, label + ".pcap")
        scapy.wrpcap(new_pcap_file_path, labeled_packets
        
"""
dataset_path = "ISCX VPN-Non VPN/"
do_packet_labeling(dataset_path)
"""        