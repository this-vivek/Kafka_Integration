# Databricks notebook source
import logging

class LogGenerator:
    """
    This class initializes logger with predefined parameters and returns logger object for further logging.
    Used through out the project.
    """
    def __init__(self,file_path = '.\logs.txt'):
        self.file_path = file_path
    
    def GetLogger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        file_handler = logging.FileHandler(self.file_path,mode = 'a')
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

