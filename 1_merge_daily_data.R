# This code creates quarterly .fst files that compress the daily .txt files FINRA sends.
# It is leveraged on previous codes by Shuo Liu and Mahyar Kargar.

# FINRA sends two types of .txt "|"-separated files: one collects transaction data 
# (terms of trade, counterparties, etc.), and one collects bond information. There is
# one file of each type for every day. The format of the data changes in 2012-02-06.

# This code creates 
# a) quarterly .fst files for the transaction data, called "trade-data-clean-YYYY-MM",where MM refers to the starting month.
# b) quarterly .fst files for bond information variables, called bond_info_XXX_YYYY-MM, where XXX = post_2012 or pre_2012.
# Year 2012 is treated differently, because of the change in data format in 2012-02-06. I create 5 files, one for January,
# and 4 for the rest of 2012.

# To run the code:
# 1) Unzip the folders with the txt files and aggregate them into yearly folders. For example, the folder 0015-CORP-2018-01-01-2018-09-30
# should contain all the txt files within 2018-01-01-2018-09-30.
# For 2012, create two folders: one for the data until February 3rd, and one for the data after February 6th.
# 2) Store these unzipped raw data folders in your directory "path_TRACE/subpath_raw_datat"
# 3) Name the sub directories in which you want to store the quarterly outputs in "Set paths" section below.
# 4) Directory "subpath_d_fst_data" is a temporary folder that is created to keep track of any error,
# if you want to keep it, comment the file.remove and unlink commands at the end of the code.

# Note: before 2012 Feb 6 change, the variables SCRTY_SBTP_CD and ACCRD_INTRS_AM, in corp_bond files, are mostly empty.
# If you want to work with these variables later, pay special attention to how are they classified when reading them from the .txt


# clear workspace
rm(list=ls()) 

# load libraries
library(data.table)
library(dplyr)
library(tictoc)
library(fst) 

# Set paths
path_TRACE <- file.path(gsub("/3-4-5 Years/Research/2_Portfolio_Trading_in_OTC/data/code","",getwd()),"TRACE")  # REPLACE
subpath_raw_data <- "1_raw_data_daily_txt"                                # REPLACE
subpath_d_fst_data <- "2_raw_data_daily_fst"                              # REPLACE (code-created folder)
subpath_q_fst_data <- "3_raw_data_quarterly_fst"                          # REPLACE (code-created folder)
subpath_q_R_data <- "3_raw_data_quarterly_R"                              # REPLACE (code-created folder)
path_daily_txt <- file.path(path_TRACE,subpath_raw_data)
path_daily_fst <- file.path(path_TRACE,subpath_d_fst_data)
path_quarterly_fst <- file.path(path_TRACE,subpath_q_fst_data)
path_quarterly_R <- file.path(path_TRACE,subpath_q_R_data)
setwd(path_daily_txt)


##############################################################################################################################
# Function to read a file and save as .fst
# The first argument, "file", is the file name.
# The second argument, "str" filters out the right files, looking for a string called str.
# The third argument, "n", filters out the legend at the end of each file, deleting n number of ending lines.
# The forth argument, "col_type_pre", contains the class of each variable pre 2012, taken from a file with no blanks in data
# The fifth argument, "col_type_post", contains the class of each variable post 2012, taken from a file with no blanks in data
##############################################################################################################################

customfread <- function(file,str,n,col_type_pre,col_type_post) {
  if (grepl(str,file)==TRUE){                                               # filter files containing str in file name 
    pos <- regexpr("20",file)                                               # find the starting character position of year in file name
    YYYYMMDD <- substr(file,pos,pos+9)                                      # create a string for the year, month, day as YYYY-MM-DD
    message("reading file : ",file)
    if ((str=="acad")&(YYYYMMDD<"2012-02-06")){                                                       # "acad" are .txt containing bond data. Apply to previous 2012 TRACE change files.
      temp <- head(fread(file, header = TRUE, sep = "|",fill= TRUE, colClasses=col_type_pre),-n)      # read file as data.table except last n rows
    }else if ((str=="acad")&(YYYYMMDD>="2012-02-06")){                                                # "acad" are .txt containing bond data. Apply to previous 2012 TRACE change files.
      temp <- head(fread(file, header = TRUE, sep = "|",fill= TRUE, colClasses=col_type_post),-n)     # read file as data.table except last n rows
    }else { 
      temp <- head(fread(file, header = TRUE, sep = "|",fill= TRUE),-n)                               # read file as data.table except last n rows
    }
    name <- gsub(".txt","",file)                                            # name = file name but without ".txt"
    assign(name,temp)                                                       # create named variable name with data.table temp
    if ((str=="bond-2")&(YYYYMMDD<"2012-02-06")){                           # "bond-2" are .txt containing bond data. Apply to previous 2012 TRACE change files.
      rm(temp)                                                              # remove temp, since it is already in name object.
      get(name)[!(is.na(CUSIP_ID)&is.na(SYM_CD))]                           # filter out rows without CUSIP_ID and without SYM_CD info.
      setnames(get(name),"SYM_CD","BOND_SYM_ID")                            # replace column name SYM_CD towards BOND_SYM_ID                           
    }else if ((str=="bond-2")&(YYYYMMDD>="2012-02-06")) {                   # "bond-2" are .txt containing bond data. Apply to post 2012 TRACE change files.
      rm(temp)
      get(name)[!(is.na(CUSIP_ID)&is.na(SYM_CD))]
      setnames(get(name),"SYM_CD","ISSUE_SYM_ID")                           # replace column name SYM_CD towards ISSUE_SYM_ID     
    }
    write.fst(get(name),
              path = file.path(path_daily_fst,dir,paste0(name,".fst")))     # save as .fst, using the same name as original file but in fst daily folder
  }
}

################################################################################################
# Function to load trade data and bond data and merge the two
# The first argument, "file", is the master file name (trade data)
# The second argument, "str", is the part of use-file name (bond data) excluding time
# The third argument, "str_join", is the matching-variables used to merge master and using files
################################################################################################

customerge <- function(file,str,str_join) {
  message("loading file : ",file)
  # load(file)
  temp <- read.fst(file, as.data.table = TRUE)
  pos <- regexpr("20",file)                                                           # find the starting character position of year in file name
  YYYYMMDD <- substr(file,pos,pos+9)                                                  # create a string for the year, month, day as YYYY-MM-DD
  name_master <- gsub(".fst","",file)                                                 # name master_file = file name but without ".fts"
  assign(name_master,temp)                                                            # create named variable name_master with data.table from trade data
  rm(temp)
  use_file <- paste0(str,YYYYMMDD,".fst")                                             # re-build name of use-file saved as .fst
  temp <- read.fst(use_file, as.data.table = TRUE)                                    # read such .fst
  name_use <- gsub(".fst","",use_file)                                                # name name_use = file name but without ".fts"
  assign(name_use,temp)                                                               # create named variable name_use with data.table from bond data
  rm(temp)
  setkeyv(get(name_master), c(str_join))                                              # sort the object to ease merging in the next step 
  setkeyv(get(name_use), c(str_join))                                                 # sort the object to ease merging in the next step
  temp <- merge(get(name_master),get(name_use), all.x = TRUE, suffixes = c("",".y"))   # create named variable temp which is the merged output of the function
  rm(list=c(name_master,name_use))                                                    # remove databases associated to the named variables in list
  return(temp)                                                            # return output of the function
}

###################################################################################################################
# Here we use created functions to merge trade and bond data for each day and combine all days into quarterly files
###################################################################################################################

# To run all data from scratch
#dir.create(path_daily_fst)                                                # create folder where to store daily .fst files
#dir.create(path_quarterly_fst)                                            # create folder where to store quarterly merged .fst files
#dir.create(path_quarterly_R)                                              # create folder where to store quarterly bond variables .R files
#dir_list <- list.files()                                                  # vector of all folders within directory path_daily_fst

# If in need of updating only a few quarters of data
dir.create(path_daily_fst)                                                # create folder where to store daily .fst files
dir_list <- c("0015-CORP-2019-01-01-2019-12-31")                          # name of folder in which you have the new data

# Get column type for each variable in pre and post trade files.
# Instead of imposing class types, we let R fread choose base on data, using files that have
# no empty columns (empty which may lead fread to assign a non wanted logical class type)
# Note 1: Daily trade files before 2016.01.01 don't include neither FIRST_TRD_CNTRL_DT nor 
# FIRST_TRD_CNTRL_NB, so have 37 variables and R give us a warning. It's OK.
# Note 2: This step is done based on Dec-19 data. If new variables were included in TRACE after that, post_path file should be updated
pre_path <- file.path(path_daily_txt,"0015-CORP-2012-01-01-2012-02-03","0015-corp-academic-trace-data-2012-02-03.txt")
post_path <- file.path(path_daily_txt,"0015-CORP-2019-01-01-2019-12-31","0015-corp-academic-trace-data-2019-12-27.txt")
pre_data <- head(fread(pre_path, header = TRUE, sep = "|",fill= TRUE),-2)
post_data <- head(fread(post_path, header = TRUE, sep = "|",fill= TRUE),-2)
col_pre <- sapply(pre_data,class)
col_post <- sapply(post_data,class)   
rm("pre_data","post_data")

for (dir in dir_list){
  if(grepl("0015-CORP-20",dir)==TRUE){                                    # just check to avoid selecting non related directories
    
    message(paste("working with directory",dir))                          # display the directory name
    dir.create(file.path(path_daily_fst,dir))                             # create folder where to store daily .fst files for specific year
    pos <- regexpr("20",dir)                                              # find the position of year in the folder string name
    
    setwd(file.path(path_daily_txt,dir))                                        # change working directory towards folder at hand
    file_list <- list.files(pattern=".txt")                                     # create the list of .txt files in the folder 
    list_of_frame <- lapply(file_list,customfread,"acad",2,col_pre,col_post)    # read all TRADE files and save as .fst in the directory using customfread(x,"acad",2), where x = file_list 
    rm(list_of_frame)                                                           # customfread already saved files in directory, so we remove list_of_frame output.
    list_of_frame <- lapply(file_list,customfread,"bond-2",2,col_pre,col_post)  # read all BOND files and save as .fst
    rm(list_of_frame)
    list_of_frame <- lapply(file_list,customfread,"bond-s",2,col_pre,col_post)  # read all BOND-supplemental files and save as .fst . Note: in our data, we do not include variables in bond-supplemental files as they are mainly covered by BOND files
    rm(list_of_frame)
    
    setwd(file.path(path_daily_fst,dir))                                  # change working directory towards folder at hand
    
    num_list <- c("01","02","03","04","05","06",
                  "07","08","09","10","11","12")
    quarter_master_list <- c()                                            # create list of characters for each month to split trade daily files into quarters.
    for (i in 1:4) {
      quarter_master_list[i] <- paste0("0015-corp-academic-trace-data-",substr(dir,pos,pos+3),"-",num_list[(i-1)*3+1],
                                      "|0015-corp-academic-trace-data-",substr(dir,pos,pos+3),"-",num_list[(i-1)*3+2],
                                      "|0015-corp-academic-trace-data-",substr(dir,pos,pos+3),"-",num_list[(i-1)*3+3])
    }
    quarter_use_list <- c()                                               # create list of characters for each month to split bond daily files into quarters.
    for (i in 1:4) {
      quarter_use_list[i] <- paste0("0015-corp-bond-",substr(dir,pos,pos+3),"-",num_list[(i-1)*3+1],
                                   "|0015-corp-bond-",substr(dir,pos,pos+3),"-",num_list[(i-1)*3+2],
                                   "|0015-corp-bond-",substr(dir,pos,pos+3),"-",num_list[(i-1)*3+3],
                                   "|0015-corp-bond-supplemental-",substr(dir,pos,pos+3),"-",num_list[(i-1)*3+1],
                                   "|0015-corp-bond-supplemental-",substr(dir,pos,pos+3),"-",num_list[(i-1)*3+2],
                                   "|0015-corp-bond-supplemental-",substr(dir,pos,pos+3),"-",num_list[(i-1)*3+3])
    }
    
    
    for (i in 1:4) {                                           # for each starting month of the quarter
    
      master_file_list <- list.files(path=file.path(path_daily_fst,dir), 
                                     pattern= quarter_master_list[i])              # create the list of trade data .fst files in the quarter
      
      if (length(master_file_list)>0) {                                     # do not consider quarters with empty data
        
      firstfile <- master_file_list[1]                                      # name of first file of directory-quarter
      pos <- regexpr("20",firstfile)
      YYYYMMDD <- substr(firstfile,pos,pos+9)                               # string for the year, month and day of that first file
      YYYYMM <- substr(firstfile,pos,pos+6)                                 # string for the year and month that first file
      
      if (YYYYMMDD<"2012-02-06"){                                           # if pre- 2012-02-06 change
        str_join <- c("CUSIP_ID","BOND_SYM_ID")                             # use "CUSIP_ID","BOND_SYM_ID" to merge
      }else if (YYYYMMDD>="2012-02-06"){                                    # if post- 2012-02-06
        str_join <- c("CUSIP_ID","ISSUE_SYM_ID")                            # use "CUSIP_ID","ISSUE_SYM_ID" to merge
      }
      list_of_frame <- lapply(master_file_list,customerge,
                            "0015-corp-bond-",str_join)                     # merge every trade daily .fst with correspondent bond-data .fst (these last ones include "0015-corp-bond-" in their names)
      trade_data <- rbindlist(list_of_frame)                                # stack all the resulting data frame together in one data.table per directory
      rm(list_of_frame)                                                     # remove the list of frame created before
    
      dim_orig <- dim(trade_data)                                           # record the dimension of the original trade data
      trade_data_clean <- trade_data[TRDG_MKT_CD!="P1"]                     # clean the data for primary market trades outside market price.
      rm(trade_data) 
      dim_clean <- dim(trade_data_clean)                                    # record the dimension of the clean data
      message("Number of occurrences of P1 = ",dim_orig[1]-dim_clean[1])    # report the number of times P1 occurred
      toc()
      message("")
      tic()
    
      message("Now save in a .fst file")
      write.fst(x = trade_data_clean,
                path = file.path(path_quarterly_fst,paste0("trade-data-clean-",YYYYMM,".fst")))  # save the data in a .fst file within quarterly folder
      rm(trade_data_clean)
    
      # save in quarterly .fst R folder the variable names in bond data that were added to the trade data files (this step can be skipped)
      bondinfo_firstname <- paste0("0015-corp-bond-",YYYYMMDD)
      if (YYYYMMDD<"2012-02-06"){                                                           # use first file to see if current folder is pre / post- 2012-02-06
        tmp <- read.fst(path = paste0(bondinfo_firstname,".fst"),as.data.table = TRUE)      # input first bond-data file of folder
        setnames(tmp,tolower(colnames(tmp)))                                                # set all columns names in lower case
        bond_info_pre_2012 <- colnames(tmp)                                                 # store column names and delete the data
        rm(tmp)
        bond_info_pre_2012 <-
          bond_info_pre_2012[-which(bond_info_pre_2012
                                  %in% c("cusip_id","bond_sym_id"))]                      # keep variable names in bond information data that not used for matching
        saveRDS(bond_info_pre_2012,
              file = file.path(path_quarterly_R,paste0("bond_info_pre_2012-",YYYYMM,".RDS")))          # save them as .RDS
      }else if (YYYYMMDD>="2012-02-06"){
        tmp <- read.fst(path = paste0(bondinfo_firstname,".fst"),as.data.table = TRUE)
        setnames(tmp,tolower(colnames(tmp)))
        bond_info_post_2012 <- colnames(tmp)
        rm(tmp)
        bond_info_post_2012 <-
         bond_info_post_2012[-which(bond_info_post_2012 
                                   %in% c("cusip_id","issue_sym_id"))]                    # same as before but taking into account different matching variable
        saveRDS(bond_info_post_2012,
              file = file.path(path_quarterly_R,paste0("bond_info_post_2012-",YYYYMM,".RDS")))
      }
      toc()
      
      file.remove(master_file_list)                                             # Remove daily trace data .fts created for each quarter
      use_file_list <- list.files(path=file.path(path_daily_fst,dir), 
                                     pattern= quarter_use_list[i])              # create the list of bond data .fst files in the quarter
      file.remove(use_file_list)                                                # Remove daily corp bond.fts created for each quarter
    }
    }
  }
}

setwd(path_TRACE)
unlink(path_daily_fst, recursive = TRUE)                                        # Remove daily .fts directory