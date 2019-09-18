import cv2 as cv
import numpy as np

import requests
import json
import os.path as osp
import os
import sys

import pandas as pd
from multiprocessing.pool import Pool

def check_and_delete_video_file(str_videoFile_path:str):
    if os.path.exists(str_videoFile_path):
        print("delte the video file", '\t', str_videoFile_path)
        os.system("rm {}".format(str_videoFile_path))
    else:
        pass

#os.system(u"ffmpeg -i video_name -vf fps=1/3 no_ext_%05d.jpg".replace('video_name', video_name_new).replace('no_ext', video_name_no_ext))
def extract_picture(str_videoFile_path:str, str_pic_path:str):
    
    vidcap = cv.VideoCapture(str_videoFile_path, )
    success,image = vidcap.read()
    if not success:
        print("Error: failed in capture a video", '\t', str_videoFile_path)

        os.sytem("echo 'Error: failed in capture a video {} {}' >> failed_info.log".format('\t', videoFile))
        check_and_delete_video_file(str_videoFile_path)
        return

    fps = int(vidcap.get(cv.CAP_PROP_FPS))
    total_f = int(vidcap.get(cv.CAP_PROP_FRAME_COUNT))

    print("fps",fps, '\t', "total_f",total_f)
    
    if fps ==0 or total_f ==0:
        check_and_delete_video_file(str_videoFile_path)
        
        print("Error fps ==0 or total_f == 0" ,'\t', str_videoFile_path)
        os.sytem("Error fps ==0 or total_f == 0 {} {}' >> failed_info.log".format('\t', str_videoFile_path))
        return

    desire_list = list(range(0, total_f, fps * 3))
    desire_list = set(desire_list)
    
    index = 1
    os.chdir(str_pic_path)

    if int(vidcap.get(cv.CAP_PROP_FOURCC)) != 27:
        for i in desire_list:
            vidcap.set(1,i-1)     
            success,image = vidcap.read(1)         # image is an array of array of [R,G,B] values

            if not success:
                print("warning: \t error in reading video")
            frameId = vidcap.get(1)                # The 0th frame is often a throw-away

            save_name = str(index).rjust(5, '0')

            # 这里用producer实现
            cv.imwrite("./{}.jpg".format(save_name), image)
            index = index + 1
            
        check_and_delete_video_file(str_videoFile_path)
    else:
        check_and_delete_video_file(str_videoFile_path)
        

# path_abs = /tmp, folder to save downloaded video
def download_file(line:str, path_abs:str):
#     try:
    line = str(line)
    url_0 = "http://10.1.201.62/api/PartSource/GetTaskFile?assetId={}".format(line)
    response = requests.get(url_0)

    url= u'oss://mgtv-media/product' + json.loads(response.text)['msg']

    video_name_old = url.split('/')[-1]
    video_name_old_del_blanket = video_name_old.replace(" ", '')
    video_name_new = line + '_' + video_name_old_del_blanket
    
    if '.ts' in video_name_new:
        print("one ts file has been detected and looped over", '\t', line)
        return

    str_videoFile_path = osp.join(path_abs, video_name_new)

    if not os.path.exists(str_videoFile_path):
        # 没有下载
        os.chdir(path_abs)
        try:
            os.system(u"~/ossutil64  cp -f \"{}\"  {}".format(url, video_name_new))
        except:
            print("downloading failed", '\t', url)
            return
    else:
        print("downloaded before.")
        check_and_delete_video_file(str_videoFile_path)

    video_name_no_ext = video_name_new.split('.')[0]
    return str_videoFile_path

    
# download and extract the video file
# def process_vid(vid:str):
#     str_vid = str(vid)
#     str_videoFile_path = download_file(str_vid, '/tmp')

#     extract_picture(str_videoFile_path)


# line = 6282691
def process_one_video(line_s):
    try:
        str_pic_path = osp.join('/devdata/videos/long_video_pic/' , str(line_s))
        if not os.path.exists(str_pic_path):
            os.makedirs(str_pic_path)

            str_vid = str(line_s)

            str_videoFile_path = download_file(str_vid, '/tmp')
            extract_picture(str_videoFile_path, str_pic_path)

        else:
            print("has been converted, hop over oen iteration in the loop", '\t',line_s)
    except:
        print("error", '\t', line_s)


if __name__ == "__main__":
    new_cid = pd.read_csv("/devdata/long_short_rel/new_cid.csv",header=0)
            
    pool = Pool(processes=4)
    bg_index_list = list(set(new_cid.vid))

    hh = pool.map(process_one_video, bg_index_list) 