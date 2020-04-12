package org.pcg.walrus.server.dao;

import java.util.List;

import org.apache.ibatis.annotations.*;
import org.pcg.walrus.server.model.Task;

/**
 * Task: t_walrus_task
 */
@Mapper
public interface TaskDao {

    @Select("SELECT * FROM t_walrus_task ORDER BY f_task_id DESC LIMIT #{limit}")
    @Results({
            @Result(property = "taskId",  column = "f_task_id"),
            @Result(property = "author",  column = "f_author"),
            @Result(property = "level",  column = "f_level"),
            @Result(property = "format",  column = "f_format"),
            @Result(property = "desc",  column = "f_desc"),
            @Result(property = "type",  column = "f_type"),
            @Result(property = "start", column = "f_time_start"),
            @Result(property = "end", column = "f_time_end"),
            @Result(property = "logic", column = "f_logic"),
            @Result(property = "table", column = "f_table"),
            @Result(property = "bday", column = "f_bday"),
            @Result(property = "eday", column = "f_eday"),
            @Result(property = "status", column = "f_status"),
            @Result(property = "saveMode", column = "f_save_mode"),
            @Result(property = "saveFormat", column = "f_save_format"),
            @Result(property = "savePath", column = "f_save_path"),
            @Result(property = "resultLines", column = "f_result_lines"),
            @Result(property = "errorCode", column = "f_error_code"),
            @Result(property = "message", column = "f_message"),
            @Result(property = "ext", column = "f_ext"),
            @Result(property = "parameter", column = "f_parameter"),
            @Result(property = "resource", column = "f_resource"),
            @Result(property = "submitType", column = "f_submit_type"),
            @Result(property = "taskPlan", column = "f_plan")
    })
    List<Task> getRecentTasks(@Param("limit") int limit);

    @Select("SELECT * FROM t_walrus_task where f_task_author=#{author} order by f_task_id desc limit #{limit}")
    @Results({
            @Result(property = "taskId",  column = "f_task_id"),
            @Result(property = "author",  column = "f_author"),
            @Result(property = "level",  column = "f_level"),
            @Result(property = "format",  column = "f_format"),
            @Result(property = "desc",  column = "f_desc"),
            @Result(property = "type",  column = "f_type"),
            @Result(property = "start", column = "f_time_start"),
            @Result(property = "end", column = "f_time_end"),
            @Result(property = "logic", column = "f_logic"),
            @Result(property = "table", column = "f_table"),
            @Result(property = "bday", column = "f_bday"),
            @Result(property = "eday", column = "f_eday"),
            @Result(property = "status", column = "f_status"),
            @Result(property = "saveMode", column = "f_save_mode"),
            @Result(property = "saveFormat", column = "f_save_format"),
            @Result(property = "savePath", column = "f_save_path"),
            @Result(property = "resultLines", column = "f_result_lines"),
            @Result(property = "errorCode", column = "f_error_code"),
            @Result(property = "message", column = "f_message"),
            @Result(property = "ext", column = "f_ext"),
            @Result(property = "parameter", column = "f_parameter"),
            @Result(property = "resource", column = "f_resource"),
            @Result(property = "submitType", column = "f_submit_type"),
            @Result(property = "taskPlan", column = "f_plan")
    })
    List<Task> getTaskByAuthor(@Param("author") String author, @Param("limit") int limit);

    @Select("SELECT * FROM t_walrus_task where f_task_id=#{id}")
    @Results({
            @Result(property = "taskId",  column = "f_task_id"),
            @Result(property = "author",  column = "f_author"),
            @Result(property = "level",  column = "f_level"),
            @Result(property = "format",  column = "f_format"),
            @Result(property = "desc",  column = "f_desc"),
            @Result(property = "type",  column = "f_type"),
            @Result(property = "start", column = "f_time_start"),
            @Result(property = "end", column = "f_time_end"),
            @Result(property = "logic", column = "f_logic"),
            @Result(property = "table", column = "f_table"),
            @Result(property = "bday", column = "f_bday"),
            @Result(property = "eday", column = "f_eday"),
            @Result(property = "status", column = "f_status"),
            @Result(property = "saveMode", column = "f_save_mode"),
            @Result(property = "saveFormat", column = "f_save_format"),
            @Result(property = "savePath", column = "f_save_path"),
            @Result(property = "resultLines", column = "f_result_lines"),
            @Result(property = "errorCode", column = "f_error_code"),
            @Result(property = "message", column = "f_message"),
            @Result(property = "ext", column = "f_ext"),
            @Result(property = "parameter", column = "f_parameter"),
            @Result(property = "resource", column = "f_resource"),
            @Result(property = "submitType", column = "f_submit_type"),
            @Result(property = "taskPlan", column = "f_plan")
    })
    Task getTask(@Param("id") long id);

    @Insert("INSERT INTO t_walrus_task(f_author,f_level,f_format,f_desc,f_type,f_logic,f_save_mode,f_save_format,f_save_path,f_submit_type,f_resource,f_parameter,f_ext,f_status) VALUES(#{author}, #{level}, #{format}, #{desc}, #{type}, #{logic}, #{saveMode}, #{saveFormat}, #{savePath}, #{submitType}, #{resource}, #{parameter}, #{ext}, #{status})")
    @Options(useGeneratedKeys = true, keyProperty = "taskId", keyColumn = "f_task_id")
    void insert(Task task);

    @Update("UPDATE t_walrus_task SET f_table=#{table},f_bday=#{bday}, f_eday=#{eday} where f_task_id=#{id}")
    void updateQuery(@Param("id") long id, @Param("table") String table, @Param("bday") String bday, @Param("eday") String eday);

    @Update("UPDATE t_walrus_task SET f_status=#{status},f_message=#{message}, f_error_code=#{error_code} where f_task_id=#{id}")
    void fail(@Param("id") long id, @Param("status") int status, @Param("error_code") int errorCode, @Param("message") String message);

    @Update("UPDATE t_walrus_task SET f_resource=#{resource} where f_task_id=#{id}")
    void updateResource(@Param("id") long id, @Param("resource") String resource);
}
