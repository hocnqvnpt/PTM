---ngay 4/5 anh Nghia chua duyet xong, doi khi n�o xong moi chay
		select * from ttkd_bct.ptm_gtgt where thang = 202407; and id in (7301445);
			select * from ttkdhcm_ktnv.amas_duan_ngoai_doanhthu ptm  where  ma_yeucau = '235382';'183656'; 
		--KTRA DU LIEU 447
		select id, ma_yeucau, ma_dichvu, trangthai_tl, dexuat, tienthu_khkt, ngaycapnhat, ngaycapnhat_tl, ngaycapnhat_khkt , ma_gd, MA_TB
		from ttkdhcm_ktnv.amas_duan_ngoai_doanhthu ptm
					
		where-- id in (10234445)  and
					 trangthai_tl=10 and dexuat=7 and ma_dichvu  not in (20,149) 
																and tienthu_khkt > 0
															    and trunc(ptm.ngaycapnhat) <= to_date('31/08/2024','dd/mm/yyyy') 			---ngay 31 thang n
															    and trunc(ngaycapnhat_tl) <= to_date('08/09/2024','dd/mm/yyyy') 			---ngay 8 thang n+ 1
															    and trunc(ngaycapnhat_khkt) <= to_date('08/09/2024','dd/mm/yyyy') 			---ngay 8 thang n+ 1
														
															    ;

		insert into ttkd_bct.ptm_gtgt
			    ( thang , id, ma_duan_banhang, ngaycn_nvptm, ngaycn_ntl, dich_vu, dichvuvt_id, loaitb_id, ma_gd, ma_tb, sohopdong, 
				  ten_kh, diachi_kh, mst,so_gt, ngay_nghiemthu, dthu_theohopdong, dthu_doitac, tendoitac, 
				  tien_tt, ngay_tt, sohoadon, tien_tt_khkt, ngay_tt_khkt, sohoadon_khkt, ngaycapnhat_khkt, chuquan_id,-- pbh_ptm_id, 
				  manv_ptm, tennv_ptm, ma_vtcv, ma_to, ten_to, ma_pb, ten_pb, loai_ld, nhomld_id, manv_hotro, tyle_hotro, tyle_hotro_nv, tyle_am, 
				  dthu_goi_goc, dthu_goi , dthu_ps, heso_dichvu, nguon, ghi_chu)
		
			    with 
							yc_dv as (select a.ma_yeucau, a.id_ycdv, a.ma_dichvu, upper(b.ten_khachhang) ten_kh
														, row_number() over(partition by a.MA_YEUCAU, a.MA_DICHVU order by a.NGAYCAPNHAT desc) rnk
											from ttkdhcm_ktnv.amas_yeucau_dichvu a
														join ttkdhcm_ktnv.amas_yeucau b on a.ma_yeucau = b.ma_yeucau
											where a.MA_HIENTRANG <> 14
											)
--											select * from yc_dv
							, ps_truong as (select yc_dv.MA_YEUCAU, yc_dv.ID_YCDV, yc_dv.ma_dichvu, yc_dv.ten_kh, c.ps_truong, c.tyle tyle_hotro, c.tyle_nhom 
														from yc_dv 
																	left join ttkdhcm_ktnv.amas_booking_presale c 
																				on yc_dv.ma_yeucau = c.ma_yeucau and yc_dv.id_ycdv = c.id_ycdv and c.ps_truong = 1
														where yc_dv.rnk = 1  --and yc_dv.MA_YEUCAU = 214147 and yc_dv.ID_YCDV = 5520329
													)
--											select * from ps_truong where MA_YEUCAU in (214147)
							, t as	 (select c.manv_presale_hrm, b.tyle_hotro/100 tyle_hotro, decode(c.tyle_am, 0, 1, c.tyle_am/100) tyle_am, b.ma_yeucau, b.ma_dichvu, c.tyle_nhom
												 , case when c.manv_presale_hrm is not null
																	then round(b.tyle_nhom/b.tyle_hotro,2) 
															else null end tyle_hotro_nv
												, c.NGAYHEN, c.NGAYCAPNHAT, c.NGAYNHANTIN_PS, c.NGAYXACNHAN
												, d.loaihinh_tb, d.dichvuvt_id, d.loaitb_id_obss
												, b.ten_kh
										 from ps_truong b
														left join ttkdhcm_ktnv.amas_booking_presale c on b.ma_yeucau = c.ma_yeucau and b.id_ycdv = c.id_ycdv and c.xacnhan = 1 and c.ps_truong = 1 and c.tyle>0
														left join ttkdhcm_ktnv.amas_loaihinh_tb d on b.ma_dichvu = d.loaitb_id
									--	where b.ma_yeucau = c.ma_yeucau and b.id_ycdv = c.id_ycdv and b.ma_dichvu = d.loaitb_id
												    
										)
--						select * from t where ma_yeucau in (162409)-- and ma_dichvu = 986
							, d as (select ptm.id, ptm.ma_yeucau, ptm.ngaycapnhat, ptm.ngaycapnhat_tl, ptm.ma_gd, ptm.ma_tb, ptm.sohopdong
												, ptm.sonha, ptm.masothue, null so_gt, ptm.ngaynghiemthu, ptm.dt_hopdong, ptm.dt_doitac, trim(substr(ptm.tendoitac,1,500)) tendoitac
												, ptm.sotienthu, ptm.ngaythanhtoan, ptm.sohoadon, ptm.tienthu_khkt, ptm.ngaythanhtoan_khkt, ptm.sohoadon_khkt, ptm.ngaycapnhat_khkt
												, ptm.manv_am manv_ptm, t.manv_presale_hrm manv_hotro, t.tyle_hotro, t.tyle_hotro_nv, t.tyle_am--, ptm.MA_YEUCAU
												, ptm.dt_goi_bsc dthu_goi_goc, ptm.dt_goi_dongia  dthu_goi, ptm.dt_goi_bsc dthu_ps, ptm.hesodichvu heso_dichvu, 'web123-ID447', ptm.ghichu_ntl
												, t.loaihinh_tb, t.dichvuvt_id, t.loaitb_id_obss, t.ten_kh
										from ttkdhcm_ktnv.amas_duan_ngoai_doanhthu ptm
													join t on ptm.ma_yeucau = t.ma_yeucau and ptm.ma_dichvu = t.ma_dichvu
												 where ptm.trangthai_tl=10 and ptm.dexuat=7 and ptm.ma_dichvu not in (20,149) 
																and ptm.tienthu_khkt > 0 			--and id in (10972465)
															    and trunc(ptm.ngaycapnhat) < trunc(sysdate, 'month') 			---ngay 1 thang n +1
															    and trunc(ngaycapnhat_tl) <= to_date('08/10/2024','dd/mm/yyyy') 			---ngay 8 thang n+ 1
															    and trunc(ngaycapnhat_khkt) <= to_date('08/10/2024','dd/mm/yyyy') 			---ngay 8 thang n+ 1
															    and not exists(select * from ttkd_bct.ptm_gtgt where id=ptm.id)                 
												 )
--												 	select * from d where id in (7301445)
--SELECT trunc(sysdate, 'month') FROM DUAL
			    select 202409 thang, d.id, d.ma_yeucau, d.ngaycapnhat, d.ngaycapnhat_tl, d.loaihinh_tb, d.dichvuvt_id, d.loaitb_id_obss, d.ma_gd
							   , d.ma_tb, d.sohopdong, d.ten_kh, d.sonha diachi_kh
							   , d.masothue mst, d.so_gt, d.ngaynghiemthu, d.dt_hopdong, d.dt_doitac, d.tendoitac
							   , d.sotienthu, d.ngaythanhtoan, d.sohoadon, d.tienthu_khkt tien_tt_khkt, d.ngaythanhtoan_khkt, d.sohoadon_khkt, d.ngaycapnhat_khkt
							   , 145 chuquan_id
							   , d.manv_ptm, e.ten_nv tennv_ptm, e.ma_vtcv, e.ma_to, e.ten_to, e.ma_pb, e.ten_pb, e.loai_ld, e.nhomld_id
							   , d.manv_hotro, d.tyle_hotro, d.tyle_hotro_nv, d.tyle_am
							   , d.dthu_goi_goc,  d.dthu_goi, d.dthu_ps, d.heso_dichvu, 'web123-ID447', d.ghichu_ntl
			
			    from d
							left join ttkd_bsc.nhanvien e on d.manv_ptm = e.ma_nv and e.thang = 202409
--				where ma_yeucau = '10972465'
			    order by id
			    ;
			 
    commit;
    rollback;
--select * from ttkd_bct.ptm_gtgt where thang=202408;  


				-- ID447			------chay dot 2 vi doi anh Nghia duyet , chay file 3_447 moi chay.
--delete from ttkd_bsc.ct_bsc_ptm; 
--		-- select * from ttkd_bsc.ct_bsc_ptm 
--        where thang_ptm = 202408 and ma_duan_banhang = 235382
-- and ma_kh='GTGT rieng';
               
			---thang n
			insert into ttkd_bsc.ct_bsc_ptm 
								   (thang_luong, thang_ptm, id_447, ma_gd,ma_kh,ma_tb, loaihd_id, sohopdong,dich_vu,tenkieu_ld,ten_tb,diachi_ld,so_gt,mst,
								   sothang_dc,ngay_bbbg,ngay_luuhs_ttkd,dichvuvt_id,loaitb_id,trangthaitb_id,doituong_id,                
								   ma_tiepthi, donvi_tt,ghi_chu,manv_hotro, tyle_hotro, doituong_kh, ma_dt_kh,manv_ptm,tennv_ptm
								   ,ma_pb,ten_pb,ma_to,ten_to,ma_vtcv,loai_ld, nhom_tiepthi
								   , ngay_tt,soseri,tien_tt, trangthai_tt_id,dthu_ps, dthu_goi_goc,dthu_goi,heso_dichvu,ma_duan_banhang,nguon, chuquan_id
								   , tyle_am, heso_hotro_nvptm, heso_hotro_nvhotro, xacnhan_khkt, thang_xacnhan_khkt)
								   
			    select 202409 thang_ins, 202409 thang_ptm, id, case when ma_gd is not null then ma_gd else sohopdong end ma_gd, 'GTGT rieng' ma_kh, ma_tb, 1 loaihd_id
							   ,sohopdong, dich_vu, 'Dat moi hop dong GTGT/CNTT', ten_kh, diachi_kh,so_gt,mst,sothang, ngay_nghiemthu, ngay_nghiemthu ngay_luuhs_ttkd, dichvuvt_id,loaitb_id
							   , 1, 21 doituong_id, manv_ptm, ten_pb, ghi_chu
							   , manv_hotro, tyle_hotro
							   , 'KHDN' doituong_kh,'dn' ma_dt_kh,manv_ptm, tennv_ptm, ma_pb, ten_pb, ma_to, ten_to, ma_vtcv, loai_ld, nhomld_id
							   ,ngay_tt, sohoadon, tien_tt, 1 trangthai_tt_id, dthu_ps, dthu_theohopdong, dthu_goi, heso_dichvu
							   ,ma_duan_banhang, nguon, chuquan_id, tyle_am, tyle_am heso_hotro_nvptm, tyle_hotro heso_hotro_nvhotro, tien_tt_khkt, to_char(ngaycapnhat_khkt, 'yyyymm')
			    from ttkd_bct.ptm_gtgt a
			    where thang = 202409 and not exists(select 1 from ttkd_bsc.ct_bsc_ptm where id_447 = a.id) 
			    ;

commit;
rollback;

