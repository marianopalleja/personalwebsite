# This code clean reporting errors for files post 2012 change.
# Read Palleja (2023) "TRACE_error_filter" for an explanation of the filter.
# Inputs: quarterly merged trade .fst files and quarterly bond characteristic .RDS files

# Note: The most updated file will contain info regarding previously reported transactions, which in turn may
# affect older previously reported transactions, and so on. To be strict, this filter needs to be run for the
# entire period we want to work with, even when updating just one quarter of data. Therefore, I allow to store
# the error filter files in a different folder, given that different projects have different time spans.

#clear work space
rm(list=ls()) 
gc()

# load libraries
library(stringr)
library(data.table)
library(lubridate)
library(tictoc)
library(dplyr)
library(zoo)
library(fst)
library(xtable)

# Set log file and storage under txt
# logfile <- file("Log_Errorfilter_post_2012-06.txt")
# sink(file = logfile,type=c("output","message"),append=FALSE,split=TRUE)

# Set paths
path_TRACE <- file.path(gsub("/3-4-5 Years/Research/2_Portfolio_Trading_in_OTC/data/code","",getwd()),"TRACE")  # REPLACE
path_databases <- file.path(gsub("/code","",getwd()),"databases")       # REPLACE.
subpath_q_fst_data <- "3_raw_data_quarterly_fst"                          # REPLACE
subpath_q_fst_ef_data <- "4_ef_data_quarterly_fst"                        # REPLACE (to be created folder)
subpath_ef_document <- "TRACE_filter"                                     # REPLACE
path_quarterly_fst <- file.path(path_TRACE,subpath_q_fst_data)
path_quarterly_ef_fst <- file.path(path_databases,subpath_q_fst_ef_data)
path_ef_document <- file.path(path_databases,subpath_ef_document)
#dir.create(path_quarterly_ef_fst)                                    # create folder where to store quarterly .fst files
setwd(path_quarterly_fst)

# Set list of files to be clean
dir_list <- sort(list.files(), decreasing = TRUE)       # list of all files from most recent to most old. Order is important for loop
dir_list <- dir_list[1:16]                              # list of files to be cleaned
dir_list_periods <- c()                               # periods of files to be cleaned
for (i in dir_list){
  pos <- regexpr("20",i)                                              # find the year in the file name
  YYYYMM <- substr(i,pos,pos+6)                                       # create a string for the year and starting month as YYYY-MM
  dir_list_periods <- append(dir_list_periods,YYYYMM)
}

# Set data.tables to be used to store non matched referring reports.
unmatched_XC2 <- data.table()
unmatched_R2 <- data.table()

# Set data table to store flow of matches
match_flow_XC <- matrix(nrow = length(dir_list_periods)+2, ncol = length(dir_list_periods))
match_flow_R <- match_flow_XC
match_flow_aux <- matrix(nrow = length(dir_list_periods)+2, ncol = 1)
match_flow_aux[nrow(match_flow_aux),1] <- "Total"
match_flow_aux[nrow(match_flow_aux)-1,1] <- "% Matched"

# Set data table to store summary statistics of filter error
counter_list <- c("period", "n_obs","n_can","n_cor","n_cor_new","n_rev","n_can_cor_net","n_can_cor_del","n_rev_net","n_rev_del")
stats <- setNames(data.table(matrix(nrow = length(dir_list), ncol = length(counter_list))), counter_list)

rm("i","pos","YYYYMM","counter_list")

###############################################################################
# Loop over all files to 
# a) erase within files cancellations, corrections and  reversals
# b) store non matched referring reports of file t to look into file t-1

tic("Entire code")
for (dir in dir_list){
  
  pos <- regexpr("20",dir)                       # find the year in the file name
  YYYYMM <- substr(dir,pos,pos+6)                # create a string for the year and month
  nbr_list <- match(dir,dir_list)                # set counter for element of dir list, useful to store stats
  stats$period[nbr_list] <- YYYYMM 
  print(paste("Cleaning file",YYYYMM))
  tic(paste("Cleaning time, file",YYYYMM))
  
  ################################################################################
  # load data base and perform preliminary adaptations 
  DATA0 <- read.fst(dir, as.data.table = TRUE)                              # load data
  setnames(DATA0,tolower(colnames(DATA0)))                                  # make all column names lower cases
  first_date <- min(DATA0$trd_rpt_dt)                                       # store oldest day of the file
  DATA0 <- DATA0[,`:=`(systm_cntrl_nb = as.numeric(systm_cntrl_nb),
                      prev_trd_cntrl_nb = as.numeric(prev_trd_cntrl_nb))]   # convert record number and previous record numbers to integers if not already
  
  DATA0$rptg_party_gvp_id <- ifelse(is.na(DATA0$rptg_party_gvp_id) | DATA0$rptg_party_gvp_id=="",
                                               DATA0$rptg_party_id, 
                                               DATA0$rptg_party_gvp_id)     # Now rptg_party_gvp_id is understood as the executing party identifier
  
  DATA0$cntra_party_gvp_id <- ifelse(is.na(DATA0$cntra_party_gvp_id) | DATA0$cntra_party_gvp_id=="",
                                               DATA0$cntra_party_id, 
                                               DATA0$cntra_party_gvp_id)    # Now cntra_party_gvp_id is understood as the executing counter-party
  
  # create day-time columns for reporting and execution in ymd_hms format
  DATA0[,datetime_rpt := 
                   paste0(trd_rpt_dt,str_pad(trd_rpt_tm,width=6,side="left",pad="0"))]   # create column for reporting date-time as YYYYMMDDHHMMSS, add 0 to left of hours (ex: 8 am not 08)
  DATA0[,datetime_rpt := ymd_hms(datetime_rpt)]                                          # convert to standard R format

  DATA0[,datetime_exctn := 
                   paste0(trd_exctn_dt,str_pad(trd_exctn_tm,width=6,side="left",pad="0"))]  # create column for execution date-time as YYYYMMDDHHMMSS, add 0 to left of hours (ex: 8 am not 08)
  DATA0[,datetime_exctn := ymd_hms(datetime_exctn)]                                         # convert to standard R format

  ##############################################################################
  # Define the variables used to match reports
  
  dn_vars <- c("cusip_id","entrd_vol_qt","rptd_pr","trd_exctn_dt","trd_exctn_tm",
               "rpt_side_cd","rptg_party_gvp_id","cntra_party_gvp_id","systm_cntrl_nb")       # List of variables used in Dick-Nielsen filter
  dn_vars_2 = dn_vars[dn_vars!="systm_cntrl_nb"]                                              # Shorter list to match reversals
  
  ##############################################################################
  # Store some stats
  
  stats$n_obs[nbr_list]      <- dim(DATA0)[1]                   # total number of reports
  stats$n_can[nbr_list]      <- sum(DATA0[,trd_st_cd]=="X")     # ref. reports cancelling within 20 days
  stats$n_cor[nbr_list]      <- sum(DATA0[,trd_st_cd]=="C")     # ref. reports cancelling through correction within 20 days (report referring to incorrect report)
  stats$n_cor_new[nbr_list]  <- sum(DATA0[,trd_st_cd]=="R")     # ref. reports correcting within 20 days (correct report)
  stats$n_rev[nbr_list]      <- sum(DATA0[,trd_st_cd]=="Y")     # ref. reversal reports (cancellations) after 20 days

  ##############################################################################
  # Start Cleaning #
 
  ########################
  # Step 1: remove past-20-day cancellation (trd_st_cd="X") and corrections (trd_st_cd="C").
  # Cancellations are reports that should not exists. Corrections are reports that 
  # should be replaced by other report (trd_st_cd="R") . In both cases original report
  # and referring report should be deleted. 

  # Order data according to reporting time. Its used to apply first-in-first-out when multiple matching.
  setkeyv(DATA0,c("trd_rpt_dt","trd_rpt_tm"))                      
  
  # Split data base into C and X referring reports and remaining 
  temp_XC <- DATA0[trd_st_cd=="X" | trd_st_cd=="C" ]               # store can or cor referring reports
  temp_XC <- rbind(temp_XC, unmatched_XC2, fill=TRUE)              # add non matched referring reports from previous files. Fill=TRUE because some variables non used in the matching where introduced over the years.
  DATA1 <- DATA0[!(trd_st_cd=="X" | trd_st_cd=="C" )]              # remove cancellations reports
  dim_DATA1 <- dim(DATA1)[1]
  rm("DATA0")
  gc()                                                             # clean memory space
  
  # Filter

    # 1st STEP : match with variables + reporting number id
    
    # generate sequence number by group to be used to remove matched reports
    temp_XC[,`:=`(count_seq=sequence(.N)),
            by=dn_vars]                                         # Create sequence of unique numbers within each of the variables specified in dn_vars
    setnames(temp_XC,"datetime_rpt","tempXC_datetime_rpt")       # change name so in match I'm sure I'm erasing reports previous to cancellation reports.  
    DATA1[,`:=`(count_seq=sequence(.N)),
          by=dn_vars]                                           # Create sequence of unique numbers within each of the variables specified in dn_vars
    
    # remove from DATA1 all rows that match with temp_X along dn_vars,  earlier reporting date, and seq number
    # Note: by matching by seq number, we assume first-in-first-out: if correcting report matches with more than one previous report, then grab the earliest one.
    DATA2   <- DATA1[!temp_XC,on=c(dn_vars,
                                   "datetime_rpt<=tempXC_datetime_rpt",
                                   "count_seq==count_seq")]
    
    # store unmatched referring report
    unmatched_XC1 <- temp_XC[!DATA1,on=c(dn_vars,
                                        "tempXC_datetime_rpt>=datetime_rpt",
                                        "count_seq==count_seq")]
    
    # 2nd STEP : match only with variables
    
    # Remove unnecessary data sets
    rm("DATA1")
    gc()
    
    # generate sequence number by group to be used to removed matched reports
    unmatched_XC1[,`:=`(count_seq=NULL)]                    # erase count_seq
    DATA2[,`:=`(count_seq=NULL)]                            # erase count_seq
    unmatched_XC1[,`:=`(count_seq=sequence(.N)),
                 by=dn_vars_2]                       
    DATA2[,`:=`(count_seq=sequence(.N)),
          by=dn_vars_2]
    
    # remove from DATA2 all rows with matching trade information and count_seq
    DATA3 <- DATA2[!unmatched_XC1,on=c(dn_vars_2,
                                       "datetime_rpt<=tempXC_datetime_rpt",
                                       "count_seq")]
    # store unmatched referring report
    unmatched_XC2 <- unmatched_XC1[!DATA2,on=c(dn_vars_2,
                                               "tempXC_datetime_rpt>=datetime_rpt", 
                                               "count_seq")]
    
  # Store flow of matches
  match_flow_aux[nbr_list,1] <- YYYYMM                      # Current period
  
  if (nbr_list==1) {
    match_flow_XC[nrow(match_flow_XC),nbr_list] <- dim(temp_XC[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[nbr_list]),"00"))])[1]    # Total number of cancellations and corrections in current period
    match_flow_XC[nbr_list,nbr_list] <- ( dim(temp_XC[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[nbr_list]),"00"))])[1] 
                                          - dim(unmatched_XC2[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[nbr_list]),"00"))])[1] )   # Matches of current period referring reports 
  } else if (nbr_list>1) {
    match_flow_XC[nrow(match_flow_XC),nbr_list] <- ( dim(temp_XC[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[nbr_list]),"00"))
                                                                 & trd_rpt_dt<as.numeric(paste0(gsub("-","",dir_list_periods[nbr_list-1]),"00"))])[1] )   # Total number of cancellations and corrections in current period
    match_flow_XC[nbr_list,1] <- ( dim(temp_XC[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[1]),"00"))])[1] 
                                          - dim(unmatched_XC2[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[1]),"00"))])[1] )   # Matches of 1st base referring reports  
    for (a in 2:nbr_list){
        match_flow_XC[nbr_list,a] <- ( dim(temp_XC[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[a]),"00"))
                                                          & trd_rpt_dt<as.numeric(paste0(gsub("-","",dir_list_periods[a-1]),"00"))])[1]
                                              - dim(unmatched_XC2[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[a]),"00"))
                                                          & trd_rpt_dt<as.numeric(paste0(gsub("-","",dir_list_periods[a-1]),"00"))])[1] )  # Matches of previous periods referring reports
      }
    }
  
  # Arrange matrices for future use
  unmatched_XC2 <- unmatched_XC2[trd_exctn_dt < first_date]      # restrict unmatched_XC2 to have referring reports with execution time before current file.
  unmatched_XC2[,`:=`(count_seq=NULL)]                           # erase count_seq
  setnames(unmatched_XC2,"tempXC_datetime_rpt","datetime_rpt")   # back to original name for datetime_rpt
  DATA3[,`:=`(count_seq=NULL)]                                   # erase count_seq
  
  # Number of cancellations and corrections that need to be filtered
  stats$n_can_cor_net[nbr_list] <- match_flow_XC[nrow(match_flow_XC),nbr_list]   # referring can|cor in current period, net of previous can|cor|rev
 
  # Remove unused db and clean memory
  rm("unmatched_XC1","temp_XC","DATA2")
  gc()

  ########################
  # Step 2: remove before past-20-day cancellation and corrections, called 
  # reversals (trd_st_cd=="Y"). We could have done this in first step, but that would
  # be a mistake if there were some reversals later cancelled by a X.
  # As with corrections, reversals also admit a following report with corrected
  # characteristics. In any case the original report and referring reversal report
  # should be deleted.
  
  # Split remaining data base into R referring reports and remaining
  temp_R <- DATA3[trd_st_cd=="Y"]                     # store reversals referring reports
  temp_R <- rbind(temp_R, unmatched_R2, fill=TRUE)    # add non matched reversals referring reports from previous files. Fill=TRUE because some variables non used in the matching where introduced over the years.
  DATA4 <- DATA3[!(trd_st_cd=="Y")]                   # remove reversals reports
  dim_DATA4 <- dim(DATA4)[1]
  rm("DATA3")
  gc()                                            
  
  # Filter
  
    # 1st STEP : match with variables + reporting number id
      
    # generate sequence number by group to be used to removed matched reports
    temp_R[,`:=`(count_seq=sequence(.N)),
           by=dn_vars_2]                       
    setnames(temp_R,c("prev_trd_cntrl_nb","datetime_rpt"),
                    c("tempR_prev_trd_cntrl_nb","tempR_datetime_rpt"))      # change name so in match I'm sure I'm erasing reports previous to cancellation reports.
    DATA4[,`:=`(count_seq=sequence(.N)),
          by=dn_vars_2]
    
    # remove from DATA4 all rows with matching trade information, reporting number id, and count_seq
    DATA5 <- DATA4[!temp_R,on=c(dn_vars_2,
                                "datetime_rpt<=tempR_datetime_rpt",
                                "systm_cntrl_nb==tempR_prev_trd_cntrl_nb",
                                "count_seq")]
    # store unmatched referring report
    unmatched_R1 <- temp_R[!DATA4,on=c(dn_vars_2,
                                       "tempR_datetime_rpt>=datetime_rpt",
                                       "tempR_prev_trd_cntrl_nb==systm_cntrl_nb",
                                       "count_seq")]
    
    # 2nd STEP : match only with variables
    
    # Remove unnecessary data sets
    rm("DATA4")
    gc()
    
    # generate sequence number by group to be used to removed matched reports
    unmatched_R1[,`:=`(count_seq=NULL)]                     # erase count_seq
    DATA5[,`:=`(count_seq=NULL)]                            # erase count_seq
    unmatched_R1[,`:=`(count_seq=sequence(.N)),
           by=dn_vars_2]                       
    DATA5[,`:=`(count_seq=sequence(.N)),
          by=dn_vars_2]
    
    # remove from DATA5 all rows with matching trade information and count_seq
    DATA6 <- DATA5[!unmatched_R1,on=c(dn_vars_2,
                                      "datetime_rpt<=tempR_datetime_rpt",
                                      "count_seq")]
    # store unmatched referring report
    unmatched_R2 <- unmatched_R1[!DATA5,on=c(dn_vars_2,
                                             "tempR_datetime_rpt>=datetime_rpt",
                                             "count_seq")]

  # Store flow of matches

  if (nbr_list==1) {
    match_flow_R[nrow(match_flow_R),nbr_list] <- dim(temp_R[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[nbr_list]),"00"))])[1]    # Total number of cancellations and corrections in current period
    match_flow_R[nbr_list,nbr_list] <- ( dim(temp_R[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[nbr_list]),"00"))])[1] 
                                            - dim(unmatched_R2[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[nbr_list]),"00"))])[1] )   # Matches of current period referring reports 
  } else if (nbr_list>1) {
    match_flow_R[nrow(match_flow_R),nbr_list] <- ( dim(temp_R[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[nbr_list]),"00"))
                                                                   & trd_rpt_dt<as.numeric(paste0(gsub("-","",dir_list_periods[nbr_list-1]),"00"))])[1] )   # Total number of cancellations and corrections in current period
    match_flow_R[nbr_list,1] <- ( dim(temp_R[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[1]),"00"))])[1] 
                                   - dim(unmatched_R2[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[1]),"00"))])[1] )   # Matches of 1st base referring reports  
    for (a in 2:nbr_list){
      match_flow_R[nbr_list,a] <- ( dim(temp_R[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[a]),"00"))
                                                   & trd_rpt_dt<as.numeric(paste0(gsub("-","",dir_list_periods[a-1]),"00"))])[1]
                                       - dim(unmatched_R2[trd_rpt_dt>as.numeric(paste0(gsub("-","",dir_list_periods[a]),"00"))
                                                           & trd_rpt_dt<as.numeric(paste0(gsub("-","",dir_list_periods[a-1]),"00"))])[1] )  # Matches of previous periods referring reports
    }
  }
  
  # Arrange matrices for future use
  unmatched_R2 <- unmatched_R2[trd_exctn_dt < first_date]  # restrict unmatched_R to have referring reports with execution time before current file.
  unmatched_R2[,`:=`(count_seq=NULL)]                      # erase count_seq
  setnames(unmatched_R2,c("tempR_prev_trd_cntrl_nb","tempR_datetime_rpt"),
                        c("prev_trd_cntrl_nb","datetime_rpt"))     # back to original name for datetime_rpt
  DATA6[,`:=`(count_seq=NULL)]                             # erase count_seq
  
  # Number of reversals that need to be filtered
  stats$n_rev_net[nbr_list] <- match_flow_R[nrow(match_flow_R),nbr_list]

  # Save error filtered files
  write.fst(DATA6,path = file.path(path_quarterly_ef_fst,
                                   paste0("errorfilter-trade-data-clean-",YYYYMM,".fst")))
  
  # Uncomment to print % unmatched reports by specific dealer causing small matching rate for reversals around 2016
  # dn_unmatched  <- unmatched_R1[!DATA5,on=c(dn_vars_2,"tempR_datetime_rpt>=datetime_rpt","count_seq")]  # Unmatched reversal reports
  # dealer_x  <- dn_unmatched[dn_unmatched$rptg_party_gvp_id=="d227d0fd448bbe76398183f61a9da7db4e7faf33"]  # Unmatched reversals uploaded by a specific dealer in certain period
  # dealer_x  <- dn_unmatched[dn_unmatched$rptg_party_gvp_id=="1082873b3e37ced5b81df37dc449e3c943effcdc"]  # Unmatched reversals uploaded by a specific dealer in certain period
  # dealer_x <- dealer_x[trd_exctn_dt >= first_date]  # Take those that should have been matched in this iteration
  # print(dim(dealer_x)[1])
  # rm("dn_unmatched","dealer_x")
  
  rm("unmatched_R1","temp_R","DATA5","DATA6")
  gc()
  
  toc(log=TRUE)
  
}

# Store unmatched reports 
write.fst(unmatched_XC2,path = file.path(path_quarterly_ef_fst,paste0("unmatched_XC2",".fst")))
write.fst(unmatched_R2,path = file.path(path_quarterly_ef_fst,paste0("unmatched_R2",".fst")))


####################################################################################
# The next lines produce the tables presented in Palleja (2023) "TRACE_error_filter".

# Store Summary stats
stats$n_can_cor_del <- data.table(colSums(match_flow_XC[1:(nrow(match_flow_XC)-2),], na.rm = T))
stats$n_rev_del <- data.table(colSums(match_flow_R[1:(nrow(match_flow_R)-2),], na.rm = T))
stats[,ptg_can_cor_del := round(100*n_can_cor_del/n_can_cor_net,digits=2)]
stats[,ptg_rev_del := round(100*n_rev_del/n_rev_net,digits=2)]
write.fst(stats,path = file.path(path_ef_document,"EF_stats_post.fst"))
stats2 <- stats[,!c("n_cor_new","n_can_cor_del","n_rev_del")]
print(xtable(stats2, type = "latex"), include.rownames=FALSE, format.args=list(big.mark=","), file = file.path(path_ef_document,"EF_stats_post.tex"))

# Store Flow stats
col_flow = ncol(match_flow_XC)
row_flow = nrow(match_flow_XC)
match_flow_XC <- matrix(as.numeric(match_flow_XC),ncol=col_flow)                                          #convert to numeric matrix
match_flow_XC[row_flow-1,] <- 100*(colSums(match_flow_XC[1:(row_flow-2),], na.rm = T)/match_flow_XC[row_flow,])   # Pctg trades matched
match_flow_XC <- setNames(data.table(match_flow_aux, match_flow_XC), c("period",dir_list_periods))         # Put into data table format
match_flow_R <- matrix(as.numeric(match_flow_R),ncol=col_flow)                                            #convert to numeric matrix
match_flow_R[row_flow-1,] <- 100*(colSums(match_flow_R[1:(row_flow-2),], na.rm = T)/match_flow_R[row_flow,])      # Pctg trades matched
match_flow_R <- setNames(data.table(match_flow_aux, match_flow_R), c("period",dir_list_periods))           # Put into data table format
write.fst(match_flow_XC,path = file.path(path_ef_document,"EF_flow_stats_XC_post.fst"))
write.fst(match_flow_R,path = file.path(path_ef_document,"EF_flow_stats_R_post.fst"))
print(xtable(match_flow_XC, type = "latex"), include.rownames=FALSE, format.args=list(big.mark=","), file = file.path(path_ef_document,"flow_stats_XC_post.tex"))
print(xtable(match_flow_R, type = "latex"), include.rownames=FALSE, format.args=list(big.mark=","), file = file.path(path_ef_document,"flow_stats_R_post.tex"))


# Print some summary statistics
tot_obs = sum(stats$n_obs)
tot_XC_tofilter = sum(stats$n_can_cor_net)
tot_XC_filtered = sum(stats$n_can_cor_del)
tot_R_tofilter = sum(stats$n_rev_net)
tot_R_filtered = sum(stats$n_rev_del)
print(paste("Total Observations: ",format(tot_obs,big.mark=",")))
tot_filtered = sum(stats$n_can)+sum(stats$n_cor)+sum(stats$n_rev)+tot_XC_filtered+tot_R_filtered
print(paste("Filtered Observations: ",format(tot_filtered,big.mark=",")))
pctg_filtered = tot_filtered/tot_obs
print(paste("% Filtered Observations: ",format(100*pctg_filtered, digits=3),"%"))
pctg_matched = (tot_XC_filtered+tot_R_filtered)/(tot_XC_tofilter+tot_R_tofilter)
print(paste("% Matched Observations: ",format(100*pctg_matched, digits=3),"%"))

toc(log=TRUE)
#sink()      # export log file