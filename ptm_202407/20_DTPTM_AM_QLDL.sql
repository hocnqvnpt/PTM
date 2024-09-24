-- Tao ds dai ly thang:

rollback;
commit;
select *  from ttkd_bsc.dm_daily_khdn 
--    update ttkd_bsc.dm_daily_khdn  set MANV_QLDAILY = 'CTV077029'
where thang=202407 and ma_daily in ('GTGT00201');,'GTGT00054','GTGT00050','GTGT00116','GTGT00136','GTGT00144')
;
   and ma_nguoigt in ('GTGT00012','GTGT00054','GTGT00050','GTGT00116','GTGT00136','GTGT00144');
alter table ttkd_bsc.dm_daily_khdn add MANV_QLDAILY       VARCHAR2(20)  ;
desc ttkd_bsc.dm_daily_khdn;
		
		----insert daily moi tu excel
		insert into ttkd_bsc.dm_daily_khdn (THANG, MA_DAILY, TEN_DAILY, MANV_QLDAILY, THANG_KYHD, LOAI_HOPDONG)		
					select 202406 thang, ma_nv  MA_DAILY, ten_nv TEN_DAILY, 'CTV077959' MANV_QLDAILY, 202406 THANG_KYHD, 'DAI LY MOI' LOAI_HOPDONG
					from admin_hcm.nhanvien where ma_nv = 'GTGT00178'
		;
		----Dai ly moi
		update  ttkd_bsc.dm_daily_khdn 
			set thang_kyhd = thang, MANV_QLDAILY= 'CTV077959', LOAI_HOPDONG = 'DAI LY MOI'
			where thang=202406 and ma_daily in ('GTGT00194')
		;
		---Dai  ly hien huu co ban moi
		update  ttkd_bsc.dm_daily_khdn 
			set thang_kyhd = thang, loai_hopdong = 'PHU LUC MOI', MANV_QLDAILY= 'VNP020233'
			where thang=202406 and ma_daily in ('GTGT00102')
		;
		---update maTo, PB, VTCV
		update ttkd_bsc.dm_daily_khdn 
			set (ma_to, ma_pb, ma_vtcv, ten_vtcv)= (select ma_to, ma_pb, ma_vtcv, ten_vtcv from ttkd_bsc.nhanvien where ma_nv = MANV_QLDAILY and thang = 202407)
			where thang=202407;
	commit;
	;
delete from ttkd_bsc.dm_daily_khdn where thang=202404  --thang n
	;
			insert into ttkd_bsc.dm_daily_khdn (thang, ma_daily,ten_daily,manv_qldaily,ma_pb, thang_kyhd,ma_to)
			    select 202405 thang, ma_daily,ten_daily,manv_qldaily,ma_pb,thang_kyhd,ma_to		---thang n
			    from ttkd_bsc.dm_daily_khdn a
			    where thang=202404 		---thang n-1
						and not exists(select 1 from ttkd_bsc.dm_daily_khdn 
											where thang=202405		---thang n 
											and ma_daily=a.ma_daily
									)
;
		

-- Dai ly moi:
		insert into ttkd_bsc.dm_daily_khdn (ma_daily,ten_daily,ma_pb, ma_to, manv_qldaily, thang, thang_kyhd, loai_hopdong )
		   
		   select distinct ma_daily, (select upper(ten_nv) from admin_hcm.nhanvien_onebss where ma_nv=a.ma_daily) ten_daily,
					 ma_pb, ma_to, ma_nv, 202405, 202405, 'DAI LY MOI'	---thang n
		    from 
					   (select distinct
									 (case when ma_tiepthi like 'GT%' or ma_tiepthi like 'DL%' then ma_tiepthi
											  when ma_nguoigt like 'GT%' or ma_nguoigt like 'DL%' then ma_nguoigt  
											  when nguoi_gt like 'GT%' or nguoi_gt like 'DL%' then nguoi_gt end) ma_daily
									 ,(case when dichvuvt_id!=2 then (select ma_dv from admin_hcm.donvi where donvi_id=a.pbh_nhan_id) 
										else ma_pb end) ma_pb, ma_to
									 ,(case when substr(ma_tiepthi,1,3) in ('VNP','CTV') then ma_tiepthi
											  when substr(ma_nguoigt,1,3) in ('VNP','CTV') then ma_nguoigt end) ma_nv                            
						  from ttkd_bsc.ct_bsc_ptm a
						  where thang_ptm =202405		--thang n
										 and (((ma_tiepthi like 'GT%' or ma_tiepthi like 'DL%') and ma_tiepthi not in (select ma_daily from ttkd_bsc.dm_daily_khdn))
												 or ((ma_nguoigt like 'GT%' or ma_nguoigt like 'DL%') and ma_nguoigt not in (select ma_daily from ttkd_bsc.dm_daily_khdn))
												 or ((nguoi_gt like 'GT%' or nguoi_gt like 'DL%') and nguoi_gt not in (select ma_daily from ttkd_bsc.dm_daily_khdn)))
						union all
								select nguoi_gt, ma_pb, '','' 
								from ttkd_bsc.dt_ptm_vnp_202405 		---thang n, file anh Tuyen tao
								where nguoi_gt like 'GT%' or nguoi_gt like 'DL%' 
					   )a 
					where ma_daily not in (select ma_daily from ttkd_bsc.dm_daily_khdn)
			   
			   ;

	--*****--Luu y khi lam IMPORT EXCEL
                MANV_QLDAILY --> theo file excle neu khac/thang
				THANG_KYHD --> neu THANG_KYHD < THANG va co dthu psc trong thang n bsc 
									--> update THANG_KYHD = THANG
													, LOAI_HOPDONG = 'PHU LUC MOI'
	

-- chi tiet tb ptm trong thang:
		---Doanh thu thuc hien do Kenh Dai ly PTM trong thang/ Doanh thu giao
		---Doanh thu thuc hien duoc ghi nhan bao gom doanh thu ban moi va doanh thu gia han cac dich vu CNTT/GTGT do kenh dai ly thuc hien trong thang
		---Doanh thu ghi nhan la doanh thu goi (khong bao gom thue), khong quy doi he so.
		-- Don vi tinh = Trieu dong
		
	select * from ttkd_bsc.ct_ptm_daily_khdn where thang=202405;

			delete from ttkd_bsc.ct_ptm_daily_khdn where thang=202405		---thang n
				;
		insert into ttkd_bsc.ct_ptm_daily_khdn 
						(thang, dich_vu, tenkieu_ld, ma_gd, ma_tb, thuebao_id, chuquan_id, ma_tiepthi, ma_nguoigt, dthu_goi, heso_quydoi, dthu_kpi, ma_nv, ma_to, ma_pb, ma_daily)
				select thang_ptm thang, dich_vu, tenkieu_ld, ma_gd, ma_tb, thuebao_id, chuquan_id, 
										  ma_tiepthi, ma_nguoigt, dthu_goi, 1 heso_quydoi, dthu_kpi, ma_nv,
										  (select ma_to from ttkd_bsc.nhanvien where thang = 202405 and ma_nv=a.ma_nv) ma_to,
										  (select ma_pb from ttkd_bsc.nhanvien where thang = 202405 and ma_nv=a.ma_nv) ma_pb, ma_daily
				from (select  thang_ptm, dich_vu, tenkieu_ld, a.ma_gd, a.ma_tb, a.thuebao_id, a.chuquan_id
													 ,a.ma_tiepthi, (case when dichvuvt_id=2 or loaitb_id=149 then a.nguoi_gt else a.ma_nguoigt end) ma_nguoigt , a.dthu_goi, a.dthu_ps
													 ,1 heso_quydoi, dthu_goi dthu_kpi  
													 ,a.ma_tiepthi ma_nv
													 ,case when loaitb_id not in (20,149) then a.ma_nguoigt else a.nguoi_gt end ma_daily  
						  from ttkd_bsc.ct_bsc_ptm a
						  where a.thang_ptm = 202405 ---THANG N
							 and (ma_tiepthi in (select ma_daily from ttkd_bsc.dm_daily_khdn where thang = a.thang_ptm )
								    or ma_nguoigt in (select ma_daily from ttkd_bsc.dm_daily_khdn where thang = a.thang_ptm) 
								    or nguoi_gt in (select ma_daily from ttkd_bsc.dm_daily_khdn where thang = a.thang_ptm) 
									)         
							 
					   union all  -- bo sung thue bao den han gia han cua thang (n+1) hay n-1
							 
					   select  thang_ptm, dich_vu, tenkieu_ld, a.ma_gd, a.ma_tb, a.thuebao_id, a.chuquan_id
												 , a.ma_tiepthi, a.ma_nguoigt , a.dthu_goi, dthu_ps_n1 dthu_ps
												 , 1 heso_quydoi, dthu_goi dthu_kpi  
												 , a.ma_tiepthi ma_nv
												 , a.ma_nguoigt ma_daily  
					   from ttkd_bsc.ct_bsc_ptm a
					   where loaihd_id = 41 
									  and (ma_tiepthi in (select ma_daily from ttkd_bsc.dm_daily_khdn where thang = a.thang_ptm  )
														or ma_nguoigt in (select ma_daily from ttkd_bsc.dm_daily_khdn where thang= a.thang_ptm ) 
														or nguoi_gt in (select ma_daily from ttkd_bsc.dm_daily_khdn where thang= a.thang_ptm )
											)
									  and dthu_ps is null and dthu_ps_n1 is not null
									  and exists (select 1 from css_hcm.db_datcoc dc
														 where thang_bd = a.thang_ptm and cuoc_dc>0 and hieuluc=1 and ttdc_id = 0 --tien_thoai is null 
																		and thuebao_id=a.thuebao_id
															)
				 
				    ) a
				    ;
		----
			update ttkd_bsc.ct_ptm_daily_khdn a
			    set (ma_nv, ma_to, ma_pb) = (select nv.ma_nv, nv.ma_to, nv.ma_pb 
																	from ttkd_bsc.dm_daily_khdn dm, ttkd_bsc.nhanvien nv 
																	where dm.thang = nv.thang and dm.manv_qldaily=nv.ma_nv and dm.thang = a.thang and dm.ma_daily=a.ma_tiepthi
																)
			    where thang = 202405 and ma_tiepthi = ma_nguoigt
					  and ma_tiepthi not like 'VNP%' and ma_nguoigt not like '%CTV%'
			;
		

-- Xac dinh AM QLDL doi voi VNPTS
		update ttkd_bsc.ct_ptm_daily_khdn a
		    set (ma_nv, ma_to, ma_pb) = (select nv.ma_nv, nv.ma_to, nv.ma_pb from ttkd_bsc.dm_daily_khdn dm, ttkd_bsc.nhanvien nv 
																where dm.thang = nv.thang and dm.manv_qldaily = nv.ma_nv and dm.thang = a.thang and dm.ma_daily = a.ma_nguoigt
															)
		    where thang = 202405 and dich_vu in ('VNPTS','VCC') and ma_tiepthi is null and ma_nv is null
			   and (ma_nguoigt like 'GTGT_%' or ma_nguoigt like 'DL_%')
		;
    
    
-- Dieu chinh do AM ALDL nhap sai tren One:
		--update ct_ptm_daily_khdn a set ma_nguoigt='GTGT00141', ma_daily='GTGT00141'
		--	where thang=202403 and ma_gd='HCM-LD/01629280'; -- tren One la GTGT00140
    
		--update ct_ptm_daily_khdn a
		--	set (ma_nv, ma_to, ma_pb)=(select nv.ma_nv, nv.ma_to, nv.ma_pb from dm_daily_khdn dm, nhanvien_202403 nv 
		--													   where dm.manv_qldaily=nv.ma_nv and dm.thang=202403 and dm.ma_daily=a.ma_nguoigt)
		--	where thang=202403 and ma_gd='HCM-LD/01629280';
   
commit;
   
-- 1/ HCM_DT_DAILY_003: Doanh thu phat sinh Kenh Dai ly
		select distinct a.*, b.*, c.ten_vtcv 
		from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien c  
		   where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv       
					and a.thang_kt is null and b.thang_kt is null 
					and a.ma_kpi='HCM_DT_DAILY_003' 
					and c.thang = 202405
					;
 
   
-- AM QLDL:
		update ttkd_bsc.bangluong_kpi_202405 a
			   set HCM_DT_DAILY_003 = null
--		    where exists (select * from blkpi_danhmuc_kpi_vtcv
--								  where thang_kt is null and ma_kpi='HCM_DT_DAILY_003' and to_truong_pho is null
--											and ma_vtcv = a.ma_vtcv)
			;
                                
			update ttkd_bsc.bangluong_kpi_202405 a
			   set HCM_DT_DAILY_003=(select round(sum(dthu_kpi)/1000000, 3) from ttkd_bsc.ct_ptm_daily_khdn
													where thang = 202405 and ma_nv = a.ma_nv)
			    where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
										  where thang_kt is null and ma_kpi='HCM_DT_DAILY_003' --and to_truong_pho is null
													and ma_vtcv=a.ma_vtcv
												) 
						 and exists (select * from ttkd_bsc.ct_ptm_daily_khdn
												where thang = 202405 and ma_nv = a.ma_nv)
			;
	
			-- Truong Line:
    
			update ttkd_bsc.bangluong_kpi_202405 a
				   set HCM_DT_DAILY_003=(select round(sum(dthu_kpi)/1000000, 3) from ttkd_bsc.ct_ptm_daily_khdn
													  where thang = 202405 and ma_to=a.ma_to)
			    where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									  where thang_kt is null and ma_kpi='HCM_DT_DAILY_003' and to_truong_pho is not null
												and ma_vtcv=a.ma_vtcv)
--						 and exists (select 1 from ttkd_bsc.bangluong_kpi_202404
--										  where ma_to=a.ma_to and ma_vtcv='VNP-HNHCM_KHDN_3.1')
							and exists (select * from ttkd_bsc.ct_ptm_daily_khdn
										where thang = 202405 and ma_to=a.ma_to)
						;

			commit;
			
				select MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO, HCM_DT_DAILY_003 
				from ttkd_bsc.bangluong_kpi_202405 a where HCM_DT_DAILY_003 is not null
				;

-- 2/ HCM_SL_DAILY_001:  So luong Dai ly hien huu co phat trien them dich vu moi va co phat sinh doanh thu trong thang
			--- So luong hop dong/ phu luc hop dong  ky ket hoan tat  giua TTKD voi Dai ly hien huu phat trien them dich vu moi va co phat sinh doanh thu trong thang/ so luong HD giao
			--- Don vi tinh: So luong hop dong		

			select distinct a.*, b.*, b.ten_vtcv from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien_202405 c  
				   where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv       
							and a.thang_kt is null and b.thang_kt is null 
							and a.ma_kpi='HCM_SL_DAILY_001' ;
 

-- AM QLDL:
				update ttkd_bsc.bangluong_kpi_202405 a
					 set HCM_SL_DAILY_001= null
--				    where exists (select * from blkpi_danhmuc_kpi_vtcv
--										  where thang_kt is null and ma_kpi='HCM_SL_DAILY_001' and to_truong_pho is null
--													and ma_vtcv=a.ma_vtcv)
--							 and exists (select * from ct_ptm_daily_khdn
--										  where thang=202403 and ma_nv=a.ma_nv_hrm)
				;
                                
                                
				update ttkd_bsc.bangluong_kpi_202405 a
				   set HCM_SL_DAILY_001=(select count(distinct ma_daily) 
																from ttkd_bsc.ct_ptm_daily_khdn b
															    where thang = 202405 and dthu_kpi>0
																				 and exists (select 1 from ttkd_bsc.dm_daily_khdn
																									 where thang_kyhd= b.thang and loai_hopdong='PHU LUC MOI' 
																													and ma_daily=b.ma_daily
																									)   
																				 and ma_nv=a.ma_nv
																)
				    -- select ma_nv, ten_nv, ten_donvi, HCM_SL_DAILY_001 from bangluong_kpi_202403 a
				    where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
										  where thang_kt is null and ma_kpi='HCM_SL_DAILY_001' --and to_truong_pho is null
													and ma_vtcv=a.ma_vtcv)
--							 and exists (select * from ttkd_bsc.ct_ptm_daily_khdn
--														where thang=202404 and ma_nv=a.ma_nv_hrm)
						;


-- Truong Line:                                    
    
			update ttkd_bsc.bangluong_kpi_202405 a
			    set HCM_SL_DAILY_001=(select count(distinct ma_daily) 
																from ttkd_bsc.ct_ptm_daily_khdn b
																    where b.thang = 202405 and dthu_kpi>0
																		 and exists (select 1 from ttkd_bsc.dm_daily_khdn
																								 where thang_kyhd= b.thang and loai_hopdong='PHU LUC MOI' 
																											and ma_daily=b.ma_daily
																						)   
																		 and ma_to=a.ma_to
														)
			    where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									  where thang_kt is null and ma_kpi='HCM_SL_DAILY_001' and to_truong_pho is not null
												and ma_vtcv=a.ma_vtcv)
--						 and exists (select * from ttkd_bsc.bangluong_kpi_202404
--										  where ma_to=a.ma_to and ma_vtcv='VNP-HNHCM_KHDN_3.1')
				;
				select MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_DONVI, TEN_DONVI, MA_TO, TEN_TO, HCM_SL_DAILY_001 
				from ttkd_bsc.bangluong_kpi_202405 a where HCM_SL_DAILY_001 is not null
				;
		
		commit;
--- 3/ HCM_SL_DAILY_002: So luong Dai ly moi phat trien va co phat sinh doanh thu trong thang
			----So luong hop dong ky ket hoan tat  giua TTKD voi Dai ly phat trien moi va co phat sinh doanh thu trong thang/ so luong HD giao
			----Don vi tinh: So luong hop dong

			select distinct a.*, b.*, c.ten_vtcv 
			from ttkd_bsc.blkpi_danhmuc_kpi a, ttkd_bsc.blkpi_danhmuc_kpi_vtcv b, ttkd_bsc.nhanvien c  
				   where a.ma_kpi=b.ma_kpi and b.ma_vtcv=c.ma_vtcv       
							and a.thang_kt is null and b.thang_kt is null 
							and a.ma_kpi='HCM_SL_DAILY_002' 
							and c.thang = 202405;
 

-- AM QLDL:
				update ttkd_bsc.bangluong_kpi_202405 a
					   set HCM_SL_DAILY_002= null
			;
					 -- select * from ct_ptm_daily_khdn where thang=202403 and ma_daily     ='GTGT00183'          ;      
      
				update ttkd_bsc.bangluong_kpi_202405 a
				   set HCM_SL_DAILY_002=(select count(distinct ma_daily) from ttkd_bsc.ct_ptm_daily_khdn b
																    where thang = 202405 and dthu_kpi>0
																		 and exists (select 1 from ttkd_bsc.dm_daily_khdn
																								 where thang_kyhd=b.thang and loai_hopdong='DAI LY MOI' 
																									and ma_daily=b.ma_daily
																							)   
																		 and ma_nv=a.ma_nv
														)
				    
				    where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
										  where thang_kt is null and ma_kpi='HCM_SL_DAILY_002' --and to_truong_pho is null
													and ma_vtcv=a.ma_vtcv)
--							 and exists (select * from ct_ptm_daily_khdn
--										  where thang=202403 and ma_nv=a.ma_nv_hrm)
						;


-- Truong Line:

    
			update ttkd_bsc.bangluong_kpi_202405 a
			    set HCM_SL_DAILY_002=(select count(distinct ma_daily) from ttkd_bsc.ct_ptm_daily_khdn b
															    where thang = 202405 and dthu_kpi>0
																	 and exists (select 1 from ttkd_bsc.dm_daily_khdn
																								 where thang_kyhd=b.thang and loai_hopdong='DAI LY MOI' 
																									and ma_daily=b.ma_daily
																						)   
																	 and ma_to=a.ma_to
														)
			    where exists (select * from ttkd_bsc.blkpi_danhmuc_kpi_vtcv
									  where thang_kt is null and ma_kpi='HCM_SL_DAILY_002' and to_truong_pho is not null
												and ma_vtcv=a.ma_vtcv)
--						 and exists (select * from ttkd_bsc.bangluong_kpi_202404
--										  where ma_to=a.ma_to and ma_vtcv in ('VNP-HNHCM_KHDN_3.1','VNP-HNHCM_KHDN_4'))
				;

                                   
commit;

				select ma_nv, ma_to, ma_vtcv, ten_nv, ten_vtcv, ten_donvi, HCM_SL_DAILY_002
				from ttkd_bsc.bangluong_kpi_202405 a
				where HCM_SL_DAILY_002 >0
					;
         

/*
-- HCM_DT_DAILY_003
select a.ten_donvi, a.ten_to, a.ma_nv_hrm,  a.ten_nv, a.ten_vtcv
            ,a.hcm_dt_daily_003 new, b.hcm_dt_daily_003 old
            ,nvl(a.hcm_dt_daily_003,0) - nvl(b.hcm_dt_daily_003,0) chechlech
    from bangluong_kpi_202403 a, bangluong_kpi_202403_l1 b 
    where a.ma_nv=b.ma_nv and nvl(a.hcm_dt_daily_003,0)<>nvl(b.hcm_dt_daily_003,0)         
    order by (a.hcm_dt_daily_003 - b.hcm_dt_daily_003);


-- HCM_SL_DAILY_001:
select a.ten_donvi, a.ten_to, a.ma_to, a.ma_nv_hrm,  a.ten_nv, a.ten_vtcv
            ,a.hcm_sl_daily_001 hcm_sl_daily_001_new, b.hcm_sl_daily_001 hcm_sl_daily_001_old,
            nvl(a.hcm_sl_daily_001,0) - nvl(b.hcm_sl_daily_001,0) chechlech
    from bangluong_kpi_202403 a, bangluong_kpi_202403_l1 b 
    where a.ma_nv=b.ma_nv and nvl(a.hcm_sl_daily_001,0)<>nvl(b.hcm_sl_daily_001,0)       
    order by (a.hcm_sl_daily_001 - b.hcm_sl_daily_001);



-- HCM_SL_DAILY_002:
select a.ten_donvi, a.ten_to, a.ma_nv_hrm,  a.ten_nv, a.ten_vtcv
            ,a.hcm_sl_daily_002 hcm_sl_daily_002_new, b.hcm_sl_daily_002 hcm_sl_daily_002_old,
            nvl(a.hcm_sl_daily_002,0) - nvl(b.hcm_sl_daily_002,0) chechlech
    from bangluong_kpi_202403 a, bangluong_kpi_202403_l1 b 
    where a.ma_nv=b.ma_nv and nvl(a.hcm_sl_daily_002,0)<>nvl(b.hcm_sl_daily_002,0)     
    order by (a.hcm_sl_daily_002 - b.hcm_sl_daily_002);

*/
