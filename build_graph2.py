# -*- coding: UTF-8 -*-
import os
import sys
import json
from py2neo import Node, Relationship, Graph, NodeMatcher, RelationshipMatcher
import MySQLdb

# graph = Graph('http://localhost:7474', username='neo4j', password='yhh')
# a = Node('label', name = 'a' )
# b = Node('label', name = 'b' )
# graph.create(a)
# graph.create(b)
# r1 = Relationship(a, 'to', b, name = 'to')
# graph.create(r1)

class FinanceCourseGraph:
    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.data_path = os.path.join(cur_dir, 'data/course_info.json')
        self.g = Graph(
            host="127.0.0.1", #localhost
            http_port=7474, #neo4j端口号
            user="neo4j", #neo4j服务器用户名
            password="yhh") #neo4j服务器密码
        print("成功连接neo4j服务器")

    def read_nodes(self):

        #预计共5类节点
        courses = [] #课程实体
        teachers = [] #教师实体
        affiliations = [] #教学机构
        points = [] #知识点s
        labels = [] #课程标签

        course_infos = [] #课程属性信息

        #构建节点实体关系
        rels_label = [] #课程-标签关系
        rels_affiliation = [] #课程-所属机构关系
        rels_courseteacher = [] #课程-授课教师关系

        count = 0

        db = MySQLdb.connect("39.100.100.198", "root", "ASElab905", "FinanceCourseKG", charset='utf8')
        cursor = db.cursor()
        sql = 'SELECT * FROM course_info'

        # 执行SQL语句
        cursor.execute(sql)
        # 获取所有记录列表
        results = cursor.fetchall()
        print('fetch sql success')
        course_dict = {}
        for row in results:
            course = row[1]
            course_dict['course_name'] = course
            courses.append(course)
            course_dict['course_id'] = ''
            course_dict['course_label'] = ''
            course_dict['course_affiliation'] = ''
            course_dict['course_teacher'] = ''
            labels.append(row[2])
            affiliations.append(row[3])
            teachers.append(row[4])
            if (row[2] is not None) :
                labels.append(row[2])
                rels_label.append([course, row[2]])
                course_dict['course_label'] = row[2]
            if row[3] is not None:
                affiliations.append(row[3])
                rels_affiliation.append([course, row[3]])
                course_dict['course_affiliation'] = row[3]
            if row[4] is not None:
                for teacherTemp in row[4].split(' '):
                    teachers.append(teacherTemp)
                    rels_courseteacher.append([course, teacherTemp])
                course_dict['course_teacher'] = row[4].split(' ')
            course_infos.append(course_dict)



        # 关闭数据库连接
        db.close()
        return set(courses), set(teachers), set(affiliations), set(labels), course_infos, \
                   rels_label, rels_affiliation, rels_courseteacher

    '''建立节点'''
    def create_node(self, label, nodes):
        print("成功执行create_node")
        count = 0
        for node_name in nodes:
            node = Node(label, name=node_name)
            self.g.create(node)
            count += 1
            print(count, len(nodes))
        return

    '''创建课程知识图谱中心课程的节点'''
    def create_courses_nodes(self, course_infos):
        print("成功执行create_courses_nodes")
        count = 0
        for course_dict in course_infos:
            node = Node("Course", name=course_dict['course_name'], label=course_dict['course_label'],
                        affiliation=course_dict['course_affiliation'], teacher=course_dict['course_teacher'])
            self.g.create(node)
            count += 1
            print(count)
        return

    '''创建课程知识图谱实体节点类型schema'''
    def create_graphnodes(self):
        print("成功执行create_graphnodes")
        Courses, Teachers, Affiliations, Labels, Course_Infos, Rels_Label, Rels_Affiliation, Rels_Courseteache = self.read_nodes()
        self.create_courses_nodes(Course_Infos)
        self.create_node('Course', Courses)
        print (len(Courses))
        self.create_node('Label', Labels)
        print (len(Labels))
        self.create_node('Teacher', Teachers)
        print (len(Teachers))
        self.create_node('Affiliation', Affiliations)
        print (len(Affiliations))
        return

    '''创建实体关联边'''
    def create_relationship(self, start_node, end_node, edges, rel_type, rel_name):
        print("成功执行create_relationship")
        count = 0
        if sys.getdefaultencoding() != 'utf-8':
            reload(sys)
            sys.setdefaultencoding('utf-8')
        # 去重处理
        set_edges = []
        for edge in edges:
            set_edges.append('###'.join(edge))
        all = len(set(set_edges))
        for edge in set(set_edges):
            edge = edge.split('###')
            p = edge[0]
            q = edge[1]
            query = "match(p:%s),(q:%s) where p.name='%s'and q.name='%s' create (p)-[rel:%s{name:'%s'}]->(q)" % (
                start_node, end_node, p, q, rel_type, rel_name)
            try:
                self.g.run(query)
                count += 1
                print(rel_type, count, all)
            except Exception as e:
                print(e)
        return

    '''创建课程知识图谱实体关联边'''
    def create_graphrels(self):
        print("成功执行create_graphrels")
        courses, labels, teachers, affiliations, course_infos, rels_label, rels_affiliation, rels_courseteacher = self.read_nodes()
        self.create_relationship('Course', 'Label', rels_label, 'course_label', '标签')
        self.create_relationship('Course', 'Teacher', rels_courseteacher, 'course_teacher', '授课教师')
        self.create_relationship('Course', 'Affiliation', rels_affiliation, 'belongs_to', '属于')


if __name__ == '__main__':
    handler = FinanceCourseGraph()
    handler.create_graphnodes()
    handler.create_graphrels()
    # handler.export_data()