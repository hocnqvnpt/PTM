drop table ttkd_bsc.nhanvien_202502_bk;
create table ttkd_bsc.nhanvien_202502_bk as
select THANG, MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2, MAIL_VNPT, SDT
				, NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID, TINH_BSC, THAYDOI_VTCV
from ttkd_bsc.nhanvien
where thang = 202502 and donvi = 'TTKD' --and ma_nv = 'VNP017378'
union all
select null, MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2, MAIL_VNPT, SDT
				, NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG, NHOMLD_ID, TINH_BSC, THAYDOI_VTCV
from nhuy.nhanvien_202502_sg a
--where ma_nv = 'VNP017378'
;
select * from ttkd_bsc.x_nhanvien_202502_giao;
select * from ttkd_bsc.x_nhanvien_202502_kogiao;

select * from ttkd_bsc.x_nhanvien_202502_dieuchuyen;
select * from ttkd_bsc.x_nhanvien_202502_moi;
select * from ttkd_bsc.x_nhanvien_202502_nghi;

select * from ttkd_bsc.nhanvien
where thang = 202412 and ma_nv in (select ma_nv from ttkd_bsc.nhanvien_202412_giao)
;

select * from ttkd_bsc.nhanvien
where thang = 202412 and ma_nv in (select ma_nv from ttkd_bsc.nhanvien_202412_kogiao);
commit;



;
-----DS nhanvien nghi viec
	drop table ttkd_bsc.x_nhanvien_202502_nghi;
create table ttkd_bsc.x_nhanvien_202502_nghi as;
		with nv_old as (select ma_nv, ten_nv, ma_vtcv, initcap(ten_vtcv) ten_vtcv, ma_to, ma_pb, ten_pb from ttkd_bsc.nhanvien_202502_bk where thang is null)
				, nv_new as (select ma_nv, ten_nv, ma_vtcv, initcap(ten_vtcv) ten_vtcv, ma_to, ma_pb, ten_pb from ttkd_bsc.nhanvien_202502_bk where thang =202502)
		select *
		from nv_old --where ma_vtcv not in ('VNP-HNHCM_KHDN_23', 'VNP-HNHCM_KDOL_17.1')
		where ma_nv not in (select ma_nv from nv_new)		
;
-----DS nhanvien moi
	drop table ttkd_bsc.x_nhanvien_202502_moi;
	create table ttkd_bsc.x_nhanvien_202502_moi as
			select * from ttkd_bsc.nhanvien
			where thang = 202502 and donvi = 'TTKD' and ma_nv not in (select ma_nv from nhuy.nhanvien_202502_sg )
		;
	insert into ttkd_bsc.nhanvien (thang, DONVI, MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2
						, MAIL_VNPT, SDT, NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG
						, NHOMLD_ID, TINH_BSC, THAYDOI_VTCV)
 
			select 202502, 'TTKD' donvi, MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2
						, MAIL_VNPT, SDT, NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG
						, NHOMLD_ID, TINH_BSC, THAYDOI_VTCV
			from ttkd_bsc.nhanvien_202502_bk a 
			where ma_nv in (select ma_nv from ttkd_bsc.x_nhanvien_202502_moi)
							and thang is null
						and ma_nv not in (select ma_nv from ttkd_bsc.nhanvien where thang = 202502)
			;			
	commit;
-----DS thay doi MA_VTCV
create table ttkd_bsc.x_nhanvien_202502_dieuchuyen as
	with nv_old as (select ma_nv, ten_nv, ma_vtcv, ma_to, ma_pb, ten_pb from ttkd_bsc.nhanvien_202502_bk where thang is null)
			, nv_new as (select ma_nv, ten_nv, ma_vtcv, ma_to, ma_pb, ten_pb from ttkd_bsc.nhanvien_202502_bk where thang = 202502)
	select *
	from nv_new --where ma_vtcv not in ('VNP-HNHCM_KHDN_23', 'VNP-HNHCM_KDOL_17.1')
	where ma_nv not in (select ma_nv from ttkd_bsc.x_nhanvien_202502_moi)
	minus 
	select *
	from nv_old

;
----DS NVvien thay doi 2 cot TINH_BSC, THAYDOI_VTCV
drop table  ttkd_bsc.x_nhanvien_202502_giao;
		create table ttkd_bsc.x_nhanvien_202502_giao as;
				with t as (select distinct MA_NV, TINH_BSC--, THAYDOI_VTCV
									from ttkd_bsc.nhanvien_202502_bk)
						select MA_NV, TEN_NV, TEN_VTCV, TEN_TO, TEN_PB, 1 TINH_BSC
						from ttkd_bsc.nhanvien_202502_bk
						where ma_nv in (
													select  MA_NV
													from t
													group by  MA_NV
													having count(*)>1)
									and thang is null and tinh_bsc = 0
						order by ma_nv, thang
		;
drop table  ttkd_bsc.x_nhanvien_202502_kogiao;
		create table ttkd_bsc.x_nhanvien_202502_kogiao as
		with t as (select distinct MA_NV, TINH_BSC
							from ttkd_bsc.nhanvien_202502_bk)
				select MA_NV, TEN_NV, TEN_VTCV, TEN_TO, TEN_PB, 0 TINH_BSC
				from ttkd_bsc.nhanvien_202502_bk
				where ma_nv in (
											select  MA_NV
											from t
											group by  MA_NV
											having count(*)>1)
							and thang is null and tinh_bsc = 1
				order by ma_nv, thang
;

---Update Nhanvien thay doi cột TINH_BSC
		merge into ttkd_bsc.nhanvien a
			using nhuy.nhanvien_202502_l3 b
			on (a.ma_nv = b.ma_nv)
		when matched then
			update set a.TINH_BSC = b.TINH_BSC
		--	select * from ttkd_bsc.nhanvien a
				where thang = 202502 and donvi = 'TTKD'
							and ma_nv in (select ma_nv from ttkd_bsc.x_nhanvien_202502_giao union all select ma_nv from ttkd_bsc.x_nhanvien_202502_kogiao)
;
rollback;
commit;

----DELETE va INSERT record Thay doi MA_VTCV
		delete from ttkd_bsc.nhanvien a
--		select a.*   from ttkd_bsc.nhanvien a
			where a.thang = 202502 and donvi = 'TTKD' and --ma_nv = 'VNP017665'
						and ma_nv in (select ma_nv from ttkd_bsc.nhanvien_202502_dieuchuyen)
		;
		insert into ttkd_bsc.nhanvien (thang, DONVI, MANV_NNL, MA_NV, TEN_NV, MA_VTCV, TEN_VTCV, MA_TO, TEN_TO, MA_PB, TEN_PB, LOAI_LD, USER_CCBS, TR_THAI, USER_CCBS2
						, MAIL_VNPT, SDT, NGAYSINH, NHANVIEN_ID, GIOITINH, CMND, USER_DHSXKD, MANV_HRM, USER_CCOS, TRANGTHAI_CCOS, USER_IPCC, PHAN_LOAI, LOAI_LAODONG
						, NHOMLD_ID, TINH_BSC, THAYDOI_VTCV)
 
			select 202502, 'TTKD' donvi, a.* 
			from ttkd_bsc.nhanvien_202502_bk a 
			where ma_nv in (select ma_nv from ttkd_bsc.x_nhanvien_202502_dieuchuyen)
							and thang is null
			;			
			  select a.*   from ttkd_bsc.nhanvien a
			where a.thang = 202502 and donvi = 'TTKD' and --ma_nv = 'VNP017665'
						and ma_nv in (select ma_nv from ttkd_bsc.nhanvien_202502_dieuchuyen)
;


select ma_nv
from ttkd_bsc.nhanvien
where thang = 202411 and donvi = 'TTKD'
group by ma_nv having count(*)>1;

commit;